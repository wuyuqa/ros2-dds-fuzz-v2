"""Explicit, no-DDS native ABI probe for an isolated provider build.

Run in a fresh process with --manifest-sha256 from the build owner's receipt.
Importing the module or validating a manifest loads no native code. The probe
never creates an endpoint and cannot prove native membership semantics.
"""
from __future__ import annotations

import argparse
import ctypes as C
import hashlib
import json
import os
from pathlib import Path
import platform
import sys

BUILD_SCHEMA = 'r2f.fastdds-peer-build/v1'


class ProbeError(ValueError):
    pass


class Result(C.Structure):
    _fields_ = [('schema_version', C.c_uint32), ('struct_size', C.c_uint32),
                ('status', C.c_int32), ('endpoint_kind', C.c_uint32),
                ('local_guid', C.c_uint8 * 16), ('required_count', C.c_uint64),
                ('copied', C.c_uint64), ('snapshot_ordinal', C.c_uint64),
                ('complete', C.c_uint32), ('reserved', C.c_uint32)]


EXPECTED_OFFSETS = dict(schema_version=0, struct_size=4, status=8, endpoint_kind=12,
                        local_guid=16, required_count=32, copied=40,
                        snapshot_ordinal=48, complete=56, reserved=60)


def file_hash(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def validate_manifest(path, expected_sha256):
    path = Path(path).resolve(strict=True)
    if not isinstance(expected_sha256, str) or len(expected_sha256) != 64:
        raise ProbeError('explicit_manifest_sha256_required')
    raw = path.read_bytes()
    actual_hash = hashlib.sha256(raw).hexdigest()
    if actual_hash != expected_sha256:
        raise ProbeError('build_manifest_hash_mismatch')
    manifest = json.loads(raw)
    if (not isinstance(manifest, dict) or manifest.get('schema') != BUILD_SCHEMA or manifest.get('status') != 'built'
            or manifest.get('observation_mode') != 'instrumentation'
            or manifest.get('vendor_version') != '2.14.7'
            or not isinstance(manifest.get('verification'), dict)
            or manifest['verification'].get('compilation') != 'passed'):
        raise ProbeError('unsupported_or_incomplete_build_manifest')
    patch = manifest.get('patch')
    if not isinstance(patch, dict) or not isinstance(patch.get('abi'), dict):
        raise ProbeError('unsupported_provider_abi_manifest')
    abi = patch['abi']
    if (abi.get('schema_version') != 1 or abi.get('result_size') != 64
            or abi.get('guid_size') != 16 or abi.get('layout_changed') is not False
            or abi.get('virtual_slots_added') is not False):
        raise ProbeError('unsupported_provider_abi_manifest')
    # Require the compiled provider, not any provider with a similar filename.
    cpp = Path(__file__).with_name('provider.cpp')
    if manifest.get('provider_source_sha256') != file_hash(cpp):
        raise ProbeError('provider_source_identity_mismatch')
    if not isinstance(manifest.get('binaries'), dict) or not isinstance(manifest.get('install'), str):
        raise ProbeError('incomplete_binary_manifest')
    for role in ('provider', 'vendor'):
        binary = manifest['binaries'].get(role, {})
        if not isinstance(binary, dict):
            raise ProbeError('incomplete_binary_manifest:' + role)
        value = binary.get('path')
        if not isinstance(value, str) or not Path(value).is_absolute():
            raise ProbeError('binary_absolute_path_required:' + role)
        binary_path = Path(value).resolve(strict=True)
        if file_hash(binary_path) != binary.get('sha256'):
            raise ProbeError('binary_hash_mismatch:' + role)
        if not binary_path.is_relative_to(Path(manifest['install']).resolve()):
            raise ProbeError('binary_outside_isolated_install:' + role)
    if 'local_api' in manifest:
        from .local_api import validate_extension
        try:
            validate_extension(manifest)
        except (ValueError, OSError, TypeError, KeyError) as exc:
            raise ProbeError('local_api_extension_invalid:' + str(exc)) from exc
    return manifest


def verify_mapped_binaries(manifest, maps_text=None):
    """Check every segment's path/device/inode and refuse a second vendor DSO."""
    text = Path('/proc/self/maps').read_text() if maps_text is None else maps_text
    expected = {role: Path(manifest['binaries'][role]['path']).resolve(strict=True)
                for role in ('vendor', 'provider')}
    seen = set()
    records = []
    for line in text.splitlines():
        fields = line.split(maxsplit=5)
        if len(fields) != 6:
            continue
        mapped = fields[5]
        name = Path(mapped).name
        role = ('provider' if name.startswith('libr2f_fastdds_peer.so') else
                'vendor' if name.startswith(('libfastrtps.so', 'libfastdds.so')) else None)
        if role is None:
            continue
        path = expected[role]
        st = path.stat()
        device = f'{os.major(st.st_dev):02x}:{os.minor(st.st_dev):02x}'
        if (mapped.endswith(' (deleted)') or Path(mapped).resolve() != path
                or fields[3] != device or fields[4] != str(st.st_ino)):
            raise ProbeError('mapped_binary_identity_mismatch:' + role)
        seen.add(role)
        records.append({'role': role, 'path': str(path), 'device': device,
                        'inode': st.st_ino, 'sha256': file_hash(path)})
    if seen != {'vendor', 'provider'}:
        raise ProbeError('required_binary_not_mapped:' + ','.join(sorted({'vendor', 'provider'} - seen)))
    unique = {r['role']: r for r in records}
    return [unique[role] for role in ('vendor', 'provider')]


def _require(condition, reason):
    if not condition:
        raise ProbeError(reason)


def run_probe(manifest_path, manifest_sha256):
    if (sys.platform != 'linux' or platform.machine() != 'x86_64'
            or sys.byteorder != 'little' or C.sizeof(C.c_void_p) != 8):
        raise ProbeError('unsupported_native_platform')
    if (C.sizeof(Result) != 64 or C.alignment(Result) != 8
            or {name: getattr(Result, name).offset for name in EXPECTED_OFFSETS} != EXPECTED_OFFSETS):
        raise ProbeError('ctypes_result_layout_mismatch')
    manifest = validate_manifest(manifest_path, manifest_sha256)
    # Explicit opt-in only. Keep both handles alive until every probe is done.
    vendor = C.CDLL(manifest['binaries']['vendor']['path'], mode=os.RTLD_LOCAL)
    provider = C.CDLL(manifest['binaries']['provider']['path'], mode=os.RTLD_LOCAL)
    mapped = verify_mapped_binaries(manifest)
    results = []
    for kind, function_name in ((1, 'r2f_fastdds_peer_writer_v1'),
                                (2, 'r2f_fastdds_peer_reader_v1')):
        fn = getattr(provider, function_name)
        fn.argtypes = [C.c_void_p, C.POINTER(C.c_uint8), C.c_uint64,
                       C.POINTER(Result), C.c_uint32]
        fn.restype = C.c_int32
        # Never dereference a non-null endpoint in this no-DDS probe.
        out = Result()
        C.memset(C.byref(out), 0xA5, C.sizeof(out))
        before = bytes(out)
        _require(fn(None, None, 0, C.byref(out), 63) == 6, 'ABI mismatch status')
        _require(bytes(out) == before, 'ABI mismatch must not touch result')
        _require(fn(None, None, 0, C.byref(out), 64) == 1, 'null endpoint status')
        _require((out.schema_version, out.struct_size, out.status, out.endpoint_kind,
                out.required_count, out.copied, out.snapshot_ordinal, out.complete,
                out.reserved) == (1, 64, 1, kind, 0, 0, 0, 0, 0), 'invalid argument receipt')
        _require(not any(out.local_guid), 'no fabricated local GUID')
        _require(fn(None, None, 0, None, 64) == 1, 'null output status')
        results.append({'function': function_name, 'null_endpoint': 'rejected',
                        'abi_mismatch': 'rejected_without_write', 'null_output': 'rejected'})
    if validate_manifest(manifest_path, manifest_sha256) != manifest:
        raise ProbeError('manifest_changed_during_probe')
    verify_mapped_binaries(manifest)
    return {'schema': 'r2f.fastdds-peer-no-dds-probe/v1', 'status': 'passed',
            'pid': os.getpid(), 'pgid': os.getpgrp(), 'sid': os.getsid(0),
            'process_start_ticks': int(Path('/proc/self/stat').read_text().rsplit(')', 1)[1].split()[19]),
            'dedicated_process_group': os.getpid() == os.getpgrp(),
            'build_manifest': str(Path(manifest_path).resolve()),
            'build_manifest_sha256': manifest_sha256, 'mapped_binaries': mapped,
            'result_size': C.sizeof(Result), 'result_offsets': EXPECTED_OFFSETS,
            'checks': results, 'dds_entities_created': False,
            'native_membership_verified': False, 'execution_evidence': False,
            'candidate_eligible': False}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--manifest', required=True, type=Path)
    parser.add_argument('--manifest-sha256', required=True)
    args = parser.parse_args(argv)
    try:
        print(json.dumps(run_probe(args.manifest, args.manifest_sha256), indent=2, sort_keys=True))
        return 0
    except (OSError, ValueError, KeyError, AssertionError, AttributeError) as exc:
        print(json.dumps({'schema': 'r2f.fastdds-peer-no-dds-probe/v1', 'status': 'failed',
                          'error': str(exc), 'execution_evidence': False}), flush=True)
        return 2


if __name__ == '__main__':
    raise SystemExit(main())
