"""Explicit Fast DDS peer instrumentation selection; no implicit native loads.

Configuration and loaded DSO identities are separate checks. A manifest only
selects a reviewed independent build; it is never evidence of a DDS match.
"""
from __future__ import annotations

import hashlib
import os
from pathlib import Path
import re

from .probe import ProbeError, validate_manifest

ORIGINAL_VENDOR = Path('/opt/ros/jazzy/lib/libfastrtps.so')
ORIGINAL_VENDOR_SHA256 = '2b80489f57d87ce4832766371c5e9f4b0c6b035dc850b284b9f6afcad4962929'
REFERENCE_KEYS = {'observation_mode', 'manifest_path', 'manifest_sha256'}
PROOF_SOURCE = 'fastdds_native_stateful_peer_snapshot'
OBSERVATION_SCHEMA = 'r2f.fastdds-native-matched-peer-observation/v1'


def manifest_for_reference(reference):
    if (not isinstance(reference, dict) or set(reference) != REFERENCE_KEYS
            or reference.get('observation_mode') != 'instrumentation'
            or not isinstance(reference.get('manifest_path'), str)
            or not Path(reference['manifest_path']).is_absolute()
            or not isinstance(reference.get('manifest_sha256'), str)
            or re.fullmatch(r'[0-9a-f]{64}', reference['manifest_sha256']) is None):
        raise ProbeError('explicit_fastdds_peer_instrumentation_reference_required')
    manifest = validate_manifest(reference['manifest_path'], reference['manifest_sha256'])
    if manifest.get('filter_trace') is not None:
        from r2f.instrumentation.fastdds_filter_trace.activation import selected_manifest
        selected_manifest(manifest)
    return manifest


def reference_from_config(config):
    mode = config.get('observation_mode', 'blackbox')
    raw = config.get('fastdds_peer_instrumentation')
    if 'observe_native_peers' in config and (
            type(config['observe_native_peers']) is not bool or mode != 'instrumentation'):
        raise ProbeError('native_peer_sampling_control_requires_explicit_instrumentation_boolean')
    if mode == 'blackbox':
        if raw is not None:
            raise ProbeError('fastdds_peer_requires_instrumentation_mode')
        return None
    if mode != 'instrumentation' or raw is None:
        raise ProbeError('topology_observation_mode_unsupported')
    if config.get('rmw') != 'rmw_fastrtps_cpp' or config.get('rmws'):
        raise ProbeError('fastdds_peer_instrumentation_requires_single_fastdds_rmw')
    if not isinstance(raw, dict) or set(raw) != {'manifest_path', 'manifest_sha256'}:
        raise ProbeError('fastdds_peer_manifest_reference_invalid')
    reference = {'observation_mode': mode, **raw}
    manifest_for_reference(reference)
    return reference


def environment_for_reference(base_env, reference):
    env = dict(base_env)
    if reference is None:
        return env
    manifest = manifest_for_reference(reference)
    if env.get('LD_PRELOAD') or env.get('LD_AUDIT'):
        raise ProbeError('instrumentation_untracked_loader_hook')
    # Override only in this explicitly selected worker environment. No mutation
    # of os.environ or installed ROS files; blackbox receives its original env.
    libdir = str(Path(manifest['binaries']['vendor']['path']).parent)
    env['LD_LIBRARY_PATH'] = os.pathsep.join([libdir, *[
        item for item in env.get('LD_LIBRARY_PATH', '').split(os.pathsep)
        if item and item != libdir]])
    if manifest.get('filter_trace') is not None:
        from r2f.instrumentation.fastdds_filter_trace.activation import selected_manifest
        env['LD_PRELOAD'] = selected_manifest(manifest)['binary']['path']
    return env


def verify_vendor_mapping(reference=None, *, maps_text=None):
    manifest = manifest_for_reference(reference) if reference is not None else None
    binary = manifest['binaries']['vendor'] if manifest else {
        'path': str(ORIGINAL_VENDOR), 'sha256': ORIGINAL_VENDOR_SHA256}
    path = Path(binary['path']).resolve(strict=True)
    before = path.stat()
    if hashlib.sha256(path.read_bytes()).hexdigest() != binary['sha256']:
        raise ProbeError('selected_vendor_binary_hash_mismatch')
    device = f'{os.major(before.st_dev):02x}:{os.minor(before.st_dev):02x}'
    text = Path('/proc/self/maps').read_text() if maps_text is None else maps_text
    seen = False
    for line in text.splitlines():
        fields = line.split(maxsplit=5)
        if len(fields) != 6:
            continue
        name = Path(fields[5]).name
        if not name.startswith(('libfastrtps.so', 'libfastdds.so')):
            continue
        if (fields[5].endswith(' (deleted)') or Path(fields[5]).resolve() != path
                or fields[3] != device or fields[4] != str(before.st_ino)):
            raise ProbeError('selected_vendor_mapping_mismatch')
        seen = True
    after = path.stat()
    if (not seen or (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns)
            != (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns)):
        raise ProbeError('selected_vendor_mapping_unavailable_or_changed')
    return {'schema': 'r2f.fastdds-worker-vendor-identity/v1',
            'observation_mode': 'instrumentation' if manifest else 'blackbox',
            'manifest_reference': dict(reference) if reference is not None else None,
            'path': str(path), 'sha256': binary['sha256'], 'device': device,
            'inode': before.st_ino, 'identity_verified': True,
            'membership_verified': False, 'candidate_eligible': False}
