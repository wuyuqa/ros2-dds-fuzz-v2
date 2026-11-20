"""Pinned optional evaluate observation; imports/validation never load a DSO."""
from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import re

SCHEMA = 'r2f.fastdds-filter-trace-build/v1'
ABI = dict(schema_version=1, record_size=4192, stats_size=48, payload_capacity=4096)


def _need(ok, reason):
    if not ok:
        raise ValueError('filter_trace:' + reason)


def _hash(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def validate_reference(reference, *, vendor_binary):
    _need(isinstance(reference, dict) and set(reference) == {'manifest_path', 'manifest_sha256'},
          'explicit_manifest_reference_required')
    name, digest = reference['manifest_path'], reference['manifest_sha256']
    _need(isinstance(name, str) and Path(name).is_absolute()
          and isinstance(digest, str) and re.fullmatch('[0-9a-f]{64}', digest) is not None,
          'manifest_reference_invalid')
    path = Path(name).resolve(strict=True)
    _need(_hash(path) == digest, 'manifest_hash_mismatch')
    manifest = json.loads(path.read_bytes())
    _need(isinstance(manifest, dict) and manifest.get('schema') == SCHEMA
          and manifest.get('status') == 'built' and manifest.get('observation_mode') == 'instrumentation'
          and isinstance(manifest.get('verification'), dict)
          and manifest['verification'].get('compilation') == 'passed'
          and manifest.get('abi') == ABI, 'manifest_contract_invalid')
    _need(manifest.get('vendor_binary') == vendor_binary, 'vendor_differs_from_selected_peer_build')
    for key, filename in (('hook_source_sha256', 'hook.cpp'), ('builder_source_sha256', 'build.py')):
        _need(manifest.get(key) == _hash(Path(__file__).with_name(filename)), 'source_identity_mismatch:' + key)
    install, binary = manifest.get('install'), manifest.get('binary')
    _need(isinstance(install, str) and Path(install).is_absolute()
          and isinstance(binary, dict) and set(binary) == {'path', 'sha256'}
          and isinstance(binary.get('path'), str) and Path(binary['path']).is_absolute(),
          'binary_identity_invalid')
    binary_path = Path(binary['path']).resolve(strict=True)
    _need(binary_path.is_relative_to(Path(install).resolve(strict=True)), 'binary_outside_isolated_install')
    _need(_hash(binary_path) == binary['sha256'], 'binary_hash_mismatch')
    # Bind the original peer build, not an application-authored vendor label.
    original = manifest.get('peer_manifest_reference')
    _need(isinstance(original, dict) and set(original) == {'manifest_path', 'manifest_sha256'},
          'peer_manifest_reference_missing')
    from r2f.instrumentation.fastdds_peer.probe import validate_manifest
    peer = validate_manifest(original['manifest_path'], original['manifest_sha256'])
    _need(peer['binaries']['vendor'] == vendor_binary and peer.get('filter_trace') is None,
          'original_peer_vendor_or_manifest_invalid')
    return manifest


def selected_manifest(peer_manifest):
    reference = peer_manifest.get('filter_trace')
    return (None if reference is None else
            validate_reference(reference, vendor_binary=peer_manifest['binaries']['vendor']))


def mapped_identity(manifest, *, maps_text=None):
    """Require this already-preloaded binary, never load one as a fallback."""
    binary = manifest['binary']
    path = Path(binary['path']).resolve(strict=True)
    before = path.stat()
    _need(_hash(path) == binary['sha256'], 'mapped_binary_hash_mismatch')
    device = f'{os.major(before.st_dev):02x}:{os.minor(before.st_dev):02x}'
    text = Path('/proc/self/maps').read_text() if maps_text is None else maps_text
    found = False
    for line in text.splitlines():
        fields = line.split(maxsplit=5)
        if len(fields) != 6 or not Path(fields[5]).name.startswith('libr2f_fastdds_filter_trace.so'):
            continue
        _need(not fields[5].endswith(' (deleted)') and Path(fields[5]).resolve() == path
              and fields[3] == device and fields[4] == str(before.st_ino), 'mapped_binary_identity_mismatch')
        found = True
    after = path.stat()
    _need(found and (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns)
          == (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns), 'mapped_binary_missing_or_changed')
    return dict(path=str(path), sha256=binary['sha256'], device=device, inode=before.st_ino)


def verify_loader_selection(peer_manifest, *, environment=None, maps_text=None):
    environment = os.environ if environment is None else environment
    trace = selected_manifest(peer_manifest)
    expected = trace['binary']['path'] if trace is not None else None
    _need(not environment.get('LD_AUDIT') and (environment.get('LD_PRELOAD') or None) == expected,
          'instrumentation_untracked_loader_hook')
    return mapped_identity(trace, maps_text=maps_text) if trace is not None else None
