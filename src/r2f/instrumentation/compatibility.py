"""Offline qualification seam shared by carrier consumers.

Only retained, pinned artifacts are read. Importing/calling this module never
builds, loads native code or starts DDS. ``artifact_qualified`` describes those
artifacts, not current process or DDS compatibility. Runtime admission requires
its own source/owner/environment gates and a normal compatibility control.
"""
from __future__ import annotations

from copy import deepcopy
import hashlib
import io
import json
import os
from pathlib import Path
import re
import tarfile

from .cyclonedds import SCHEMA as CYCLONE_SCHEMA
from .fastdds_peer import patch_vendor as peer_patch
from .fastdds_peer.activation import manifest_for_reference
from .fastdds_peer.probe import EXPECTED_OFFSETS, ProbeError
from .fastdds_peer.patch_vendor import (PATCH_ANCHORS, PATCH_ID, REFERENCE_ARCHIVE_SHA256,
                                       SOURCE_SHA256, VENDOR_VERSION, patch_bytes)


def _digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(',', ':')).encode()).hexdigest()


def _sha256(value):
    return isinstance(value, str) and re.fullmatch('[0-9a-f]{64}', value) is not None


def _source_inventory(source):
    if not isinstance(source, dict) or not isinstance(source.get('files'), dict):
        return False
    files = source['files']
    return (bool(files) and all(
        isinstance(name, str) and bool(Path(name).parts) and not Path(name).is_absolute()
        and '..' not in Path(name).parts and Path(name).as_posix() == name and _sha256(digest)
        for name, digest in files.items()) and source.get('sha256') == _digest(files))


def _read(reference, *, json_data=True):
    if (not isinstance(reference, dict) or set(reference) != {'path', 'sha256'}
            or not isinstance(reference['path'], str) or not Path(reference['path']).is_absolute()
            or not isinstance(reference['sha256'], str)
            or re.fullmatch('[0-9a-f]{64}', reference['sha256']) is None):
        raise ValueError('invalid_reference')
    path = Path(reference['path'])
    before = path.stat()
    raw = path.read_bytes()
    after = path.stat()
    def identity(stat):
        return stat.st_dev, stat.st_ino, stat.st_size, stat.st_mtime_ns, stat.st_ctime_ns
    if identity(before) != identity(after):
        raise ValueError('artifact_changed_during_read')
    if hashlib.sha256(raw).hexdigest() != reference['sha256']:
        raise ValueError('artifact_hash_mismatch')
    if not json_data:
        return raw
    value = json.loads(raw)
    if not isinstance(value, dict):
        raise ValueError('artifact_object_required')
    return value


def _patch_matches_source(build):
    patch, source = build.get('patch'), build.get('source_frozen')
    root = build.get('source')
    if (not isinstance(patch, dict) or not _source_inventory(source)
            or not isinstance(root, str) or not Path(root).is_absolute()
            or type(patch.get('schema_version')) is not int or patch['schema_version'] != 1
            or patch.get('patch_id') != PATCH_ID or patch.get('vendor_version') != VENDOR_VERSION
            or patch.get('vendor_version') != build.get('vendor_version')
            or patch.get('source_root') != root or patch.get('source_state') != 'patched'
            or patch.get('reference_archive_sha256') != REFERENCE_ARCHIVE_SHA256
            or patch.get('provider_sha256') != build.get('provider_source_sha256')
            or patch.get('patcher_sha256') != hashlib.sha256(Path(peer_patch.__file__).read_bytes()).hexdigest()
            or not isinstance(patch.get('files'), dict) or set(patch['files']) != set(SOURCE_SHA256)):
        return False
    for name, original in SOURCE_SHA256.items():
        record = patch['files'][name]
        target = name in PATCH_ANCHORS
        keys = {'original_sha256', 'sha256', 'patch_target'} | ({'patched_sha256'} if target else set())
        if (not isinstance(record, dict) or set(record) != keys
                or record.get('patch_target') is not target
                or record.get('original_sha256') != original or not _sha256(record.get('sha256'))
                or source['files'].get(name) != record['sha256']
                or record['sha256'] != (record.get('patched_sha256') if target else original)):
            return False
    return True


def _source_matches_archive(build):
    """Bind receipts to retained archive bytes, never to a live source tree."""
    if (build.get('archive_sha256') != REFERENCE_ARCHIVE_SHA256
            or not _source_inventory(build.get('source_frozen'))
            or not _source_inventory(build.get('source_before'))):
        return False
    raw = _read({'path': build.get('archive'), 'sha256': build.get('archive_sha256')}, json_data=False)
    expected, before = {}, build['source_before']['files']
    with tarfile.open(fileobj=io.BytesIO(raw)) as stream:
        for member in stream:
            parts = Path(member.name).parts
            if not parts or parts[0] != 'Fast-DDS-' + VENDOR_VERSION or '..' in parts:
                return False
            if member.isdir():
                continue
            name = str(Path(*parts[1:]))
            if not member.isfile() or name in expected:
                return False
            data = stream.extractfile(member).read()
            original = hashlib.sha256(data).hexdigest()
            patched = patch_bytes(name, data) if name in PATCH_ANCHORS else data
            expected[name] = hashlib.sha256(patched).hexdigest()
            if before.get(name) not in (original, expected[name]):
                return False
    return (set(before) == set(expected) and build['source_frozen']['files'] == expected)


def _fastdds(result, probe, build_reference):
    if result['observation_mode'] != 'instrumentation' or result['carrier'] != 'native':
        result['issues'].append('adapter_mode_or_carrier_unverified')
        return
    if build_reference is None:
        result['issues'].append('build_reference_missing')
        return
    build = _read(build_reference)
    identities = result['identities']
    identities['build'] = deepcopy(build_reference)
    identities['build_status'] = build.get('status', 'unknown')
    identities['binaries'] = deepcopy(build.get('binaries'))
    identities['source'] = deepcopy(build.get('source_frozen'))
    identities['source_root'] = build.get('source')
    identities['source_archive'] = {'path': build.get('archive'), 'sha256': build.get('archive_sha256')}
    process = build.get('process')
    identities['build_process'] = deepcopy(process)
    identities['patch'] = deepcopy(build.get('patch'))
    identities['patch_sha256'] = _digest(build.get('patch'))
    identities['build_error'] = deepcopy(build.get('error'))
    identities['build_error_type'] = deepcopy(build.get('error_type'))
    commands = build.get('commands')
    identities['build_commands'] = deepcopy(commands)
    if build.get('error') not in (None, ''):
        result['issues'].append('build_failure_recorded')
    # Older retained manifests may omit commands; supplied receipts must be verifiable.
    if 'commands' in build and (not isinstance(commands, list) or not commands):
        result['issues'].append('build_commands_invalid')
    for index, command in enumerate(commands if isinstance(commands, list) else []):
        if not isinstance(command, dict):
            result['issues'].append('build_command_invalid:' + str(index))
            continue
        argv = command.get('argv')
        if (not isinstance(command.get('name'), str) or not command['name'].strip()
                or not isinstance(argv, list) or not argv
                or not all(isinstance(argument, str) for argument in argv) or not argv[0]):
            result['issues'].append('build_command_invalid:' + str(index))
        if type(command.get('returncode')) is not int or command['returncode'] != 0:
            result['issues'].append('build_command_failed:' + str(command.get('name', 'unknown'))
                                    + ':' + str(command.get('returncode')))
    manifest_for_reference({'observation_mode': 'instrumentation',
                            'manifest_path': build_reference['path'],
                            'manifest_sha256': build_reference['sha256']})
    if (not isinstance(process, dict)
            or any(type(process.get(key)) is not int or process[key] <= 0
                   for key in ('pid', 'pgid', 'sid', 'start_ticks'))
            or process.get('dedicated_process_group') is not True
            or process['pid'] != process['pgid']):
        result['issues'].append('build_process_identity_incomplete')
    identities['toolchain'] = deepcopy(build.get('toolchain'))
    source = build.get('source_frozen')
    if not _source_inventory(source) or source != build.get('source_after'):
        result['issues'].append('source_build_identity_unverified')
    try:
        if not _source_matches_archive(build):
            result['issues'].append('source_archive_binding_unverified')
    except (OSError, ValueError, tarfile.TarError) as exc:
        result['issues'].append('source_archive_binding_unverified')
        result['issues'].append('source_archive_rejected:' + type(exc).__name__ + ':' + str(exc))
    if not _patch_matches_source(build):
        result['issues'].append('patch_source_binding_unverified')
    for name, digest_key in (('compile_commands.json', 'compile_commands_sha256'),
                             ('CMakeCache.txt', 'cmake_cache_sha256')):
        root = build.get('build')
        if not isinstance(root, str) or not Path(root).is_absolute():
            raise ValueError('build_root_unavailable')
        _read({'path': str(Path(root) / name), 'sha256': build.get(digest_key)}, json_data=False)
    if (probe.get('schema') != 'r2f.fastdds-peer-no-dds-probe/v1'
            or probe.get('status') != 'passed'
            or probe.get('build_manifest') != build_reference['path']
            or probe.get('build_manifest_sha256') != build_reference['sha256']
            or probe.get('dds_entities_created') is not False
            or probe.get('execution_evidence') is not False):
        result['issues'].append('probe_build_binding_or_scope_invalid')
        return
    expected_checks = [{'function': function, 'null_endpoint': 'rejected',
                        'null_output': 'rejected', 'abi_mismatch': 'rejected_without_write'}
                       for function in ('r2f_fastdds_peer_writer_v1', 'r2f_fastdds_peer_reader_v1')]
    if (probe.get('result_size') != 64 or probe.get('result_offsets') != EXPECTED_OFFSETS
            or probe.get('checks') != expected_checks):
        result['issues'].append('observer_api_incompatible_or_unverified')
    else:
        result['observer_adaptation'] = {'adapter': 'fastdds_peer/v1',
                                         'status': 'abi_probe_verified',
                                         'semantic_effect': 'not_evaluated'}
    process = {key: probe.get(key) for key in ('pid', 'pgid', 'sid', 'process_start_ticks')}
    identities['process'] = process
    if (any(type(value) is not int or value <= 0 for value in process.values())
            or probe.get('dedicated_process_group') is not True
            or process['pid'] != process['pgid']):
        result['issues'].append('probe_process_identity_incomplete')
    mapped = probe.get('mapped_binaries')
    expected = build['binaries']
    if not isinstance(mapped, list) or len(mapped) != len(expected):
        result['issues'].append('probe_binary_identity_mismatch')
    else:
        seen = set()
        for record in mapped:
            role = record.get('role') if isinstance(record, dict) else None
            if role not in expected or role in seen:
                result['issues'].append('probe_binary_identity_mismatch')
                break
            seen.add(role)
            binary = expected[role]
            path = Path(binary['path']).resolve(strict=True)
            stat = path.stat()
            if (record.get('path') != str(path) or record.get('sha256') != binary['sha256']
                    or record.get('inode') != stat.st_ino
                    or record.get('device') != f'{os.major(stat.st_dev):02x}:{os.minor(stat.st_dev):02x}'):
                result['issues'].append('probe_binary_identity_mismatch')
                break


def _cyclone(result, probe):
    if result['carrier'] != 'ros':
        result['issues'].append('adapter_mode_or_carrier_unverified')
        return
    if (probe.get('schema') != CYCLONE_SCHEMA or probe.get('scope') != 'loader_probe_only'
            or probe.get('participant_created') is not False
            or probe.get('cyclonedds_version') != result['version']
            or probe.get('available') is not True or probe.get('probe_exit_code') != 0):
        result['issues'].append('loader_probe_contract_invalid')
        return
    for key in ('installed_config_header', 'installed_version_header'):
        _read(probe.get(key), json_data=False)
    loaded = probe.get('loaded_provenance', {})
    if not isinstance(loaded, dict):
        result['issues'].append('probe_process_identity_incomplete')
        return
    result['identities']['process'] = deepcopy(loaded)
    result['identities']['binaries'] = deepcopy(loaded.get('loaded_libraries', []))
    result['identities']['source'] = None
    result['identities']['build'] = None
    result['identities']['patch'] = None
    if loaded.get('process_identity_stable') is not True or loaded.get('vendor_loaded') is not True:
        result['issues'].append('probe_process_identity_incomplete')
    # Config tokens plus a successful loader do not verify the trace parser API,
    # source equivalence, a blackbox process environment or normal DDS behavior.
    result['issues'].extend(['source_build_patch_identity_unavailable',
                             'observer_adaptation_unverified'])


def qualify_carrier(*, vendor, version, observation_mode, carrier,
                    probe_reference=None, build_reference=None, dependencies=()):
    """Check retained build/probe evidence for one explicit carrier selection.

    References are absolute ``path`` plus byte ``sha256``; dependencies are a
    sequence of such references. Current adapters: Fast DDS 2.14.7 native peer
    ABI probe (instrumentation), Cyclone 0.10.5 ROS loader (incomplete provenance).
    Other versions/paths/modes remain unqualified. Dependency file identity is
    recorded separately from loading/normal compatibility, which require CP2.
    Missing or incompatible artifacts yield named issues; no fallback version,
    mode or backend is selected. Inputs and original evidence are never changed.
    """
    result = {
        'schema': 'r2f.carrier-compatibility/v1',
        'vendor': vendor, 'version': version, 'carrier': carrier,
        'observation_mode': observation_mode, 'scope': 'retained_artifacts_only',
        'status': 'unqualified', 'run_eligible': False, 'execution_evidence': False,
        'live_compatibility': 'not_evaluated', 'dependency_load_binding': 'not_evaluated',
        'dependency_completeness': 'not_evaluated',
        'observer_adaptation': {'status': 'unverified'},
        'identities': {'source': None, 'dependencies': [], 'build': None,
                       'binaries': None, 'patch': None, 'process': None, 'probe': None},
        'issues': [],
    }
    if observation_mode not in ('blackbox', 'instrumentation') or carrier not in ('ros', 'native', 'binding'):
        result['issues'].append('selection_invalid')
    if (vendor, version) not in (('fastdds', '2.14.7'), ('cyclonedds', '0.10.5')):
        result['issues'].append('version_adapter_unverified')
    if observation_mode == 'blackbox' and build_reference is not None:
        result['issues'].append('blackbox_instrumentation_build_forbidden')
    selection_valid = not result['issues']
    try:
        if not isinstance(dependencies, (tuple, list)) or not dependencies:
            result['issues'].append('dependency_identity_unavailable')
        else:
            for reference in dependencies:
                _read(reference, json_data=False)
            result['identities']['dependencies'] = deepcopy(list(dependencies))
        if probe_reference is None:
            result['issues'].append('probe_reference_missing')
        else:
            probe = _read(probe_reference)
            result['identities']['probe'] = deepcopy(probe_reference)
            if selection_valid:
                if vendor == 'fastdds':
                    _fastdds(result, probe, build_reference)
                else:
                    _cyclone(result, probe)
    except (OSError, ValueError, TypeError, KeyError, ProbeError) as exc:
        result['issues'].append('artifact_rejected:' + type(exc).__name__ + ':' + str(exc))
    result['issues'] = list(dict.fromkeys(result['issues']))
    if not result['issues']:
        result['status'] = 'artifact_qualified'
    return result
