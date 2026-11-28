"""Pin an independently built PDP interposer without loading native code."""
from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import re

SCHEMA = 'r2f.fastdds-pdp-trace-build/v1'
ABI = dict(schema_version=1, record_size=96, stats_size=48, capacity=4096)


def need(ok, reason):
    if not ok:
        raise ValueError('pdp_trace:' + reason)


def digest(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def selected_manifest(peer):
    ref = peer.get('pdp_trace')
    if ref is None:
        return None
    need(peer.get('filter_trace') is None, 'combined_interposers_not_supported')
    need(isinstance(ref, dict) and set(ref) == {'manifest_path', 'manifest_sha256'}, 'explicit_reference_required')
    name, expected = ref['manifest_path'], ref['manifest_sha256']
    need(isinstance(name, str) and Path(name).is_absolute() and isinstance(expected, str)
         and re.fullmatch('[0-9a-f]{64}', expected), 'reference_invalid')
    path = Path(name).resolve(strict=True)
    need(digest(path) == expected, 'manifest_hash_mismatch')
    value = json.loads(path.read_bytes())
    need(isinstance(value, dict) and value.get('schema') == SCHEMA and value.get('status') == 'built'
         and value.get('observation_mode') == 'instrumentation' and value.get('abi') == ABI
         and value.get('verification', {}).get('compilation') == 'passed'
         and type(value.get('capture_enabled')) is bool, 'manifest_contract_invalid')
    need(value.get('vendor_binary') == peer['binaries']['vendor'], 'vendor_identity_mismatch')
    for key, filename in [('hook_source_sha256', 'hook.cpp'), ('builder_source_sha256', 'build.py')]:
        need(value.get(key) == digest(Path(__file__).with_name(filename)), 'source_identity_mismatch:' + key)
    binary = value.get('binary', {})
    need(isinstance(value.get('install'), str) and Path(value['install']).is_absolute()
         and isinstance(binary, dict) and set(binary) == {'path', 'sha256'}
         and isinstance(binary.get('path'), str) and Path(binary['path']).is_absolute(), 'binary_identity_invalid')
    binary_path = Path(binary['path']).resolve(strict=True)
    need(binary_path.is_relative_to(Path(value['install']).resolve(strict=True))
         and digest(binary_path) == binary['sha256'], 'binary_hash_or_install_mismatch')
    control=value.get('refresh_control')
    if control is not None:
        need(isinstance(control,dict) and control.get('schema')=='r2f.fastdds-participant-refresh-build/v1'
             and control.get('source_sha256')==digest(Path(__file__).with_name('refresh.cpp'))
             and control.get('abi_size')==576 and control.get('compilation')=='passed', 'refresh_control_contract_invalid')
        selected=control.get('binary',{})
        need(isinstance(selected,dict) and set(selected)=={'path','sha256'}
             and isinstance(selected.get('path'),str) and Path(selected['path']).is_absolute(), 'refresh_control_binary_invalid')
        selected_path=Path(selected['path']).resolve(strict=True)
        need(selected_path.is_relative_to(Path(value['install']).resolve(strict=True))
             and digest(selected_path)==selected['sha256'], 'refresh_control_binary_hash_mismatch')
    original = value.get('peer_manifest_reference', {})
    need(isinstance(original, dict) and set(original) == {'manifest_path', 'manifest_sha256'}, 'peer_reference_invalid')
    from r2f.instrumentation.fastdds_peer.probe import validate_manifest
    base = validate_manifest(original['manifest_path'], original['manifest_sha256'])
    need(base['binaries'] == peer['binaries'] and base.get('pdp_trace') is None
         and base.get('filter_trace') is None, 'base_peer_identity_mismatch')
    return value


def mapped_identity(value, *, maps_text=None):
    binary = value['binary']; path = Path(binary['path']).resolve(strict=True); before = path.stat()
    device = f'{os.major(before.st_dev):02x}:{os.minor(before.st_dev):02x}'
    need(digest(path) == binary['sha256'], 'mapped_binary_hash_mismatch')
    text = Path('/proc/self/maps').read_text() if maps_text is None else maps_text
    found = False
    for line in text.splitlines():
        fields = line.split(maxsplit=5)
        if len(fields) != 6 or not Path(fields[5]).name.startswith('libr2f_fastdds_pdp_trace.so'):
            continue
        need(not fields[5].endswith(' (deleted)') and Path(fields[5]).resolve() == path
             and fields[3] == device and fields[4] == str(before.st_ino), 'mapped_binary_identity_mismatch')
        found = True
    after = path.stat()
    need(found and (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns)
         == (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns), 'mapped_binary_missing_or_changed')
    return dict(path=str(path), sha256=binary['sha256'], device=device, inode=before.st_ino)


def verify_loader_selection(peer, *, environment=None, maps_text=None):
    env = os.environ if environment is None else environment
    value = selected_manifest(peer)
    need(value is not None and not env.get('LD_AUDIT') and env.get('LD_PRELOAD') == value['binary']['path']
         and env.get('R2F_FASTDDS_PDP_TRACE_ENABLED') == ('1' if value['capture_enabled'] else '0'), 'loader_selection_mismatch')
    return mapped_identity(value, maps_text=maps_text)
