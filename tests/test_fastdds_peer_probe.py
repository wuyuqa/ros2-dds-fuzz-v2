"""Manifest and ELF-map identity checks, without loading a DSO or creating DDS."""
from copy import deepcopy
import ctypes
import json
import os
from pathlib import Path

import pytest

from r2f.instrumentation.fastdds_peer import probe as p


@pytest.fixture
def build_manifest(tmp_path):
    install = tmp_path / 'install'
    lib = install / 'lib'
    lib.mkdir(parents=True)
    binaries = {}
    for role, name in (('vendor', 'libfastrtps.so.2.14.7'), ('provider', 'libr2f_fastdds_peer.so')):
        path = lib / name
        path.write_bytes(('fixture ' + role).encode())
        binaries[role] = {'path': str(path), 'sha256': p.file_hash(path)}
    value = {'schema': p.BUILD_SCHEMA, 'status': 'built',
             'observation_mode': 'instrumentation', 'vendor_version': '2.14.7',
             'verification': {'compilation': 'passed'}, 'install': str(install),
             'provider_source_sha256': p.file_hash(Path(p.__file__).with_name('provider.cpp')),
             'patch': {'abi': {'schema_version': 1, 'result_size': 64, 'guid_size': 16,
                               'layout_changed': False, 'virtual_slots_added': False}},
             'binaries': binaries}
    path = tmp_path / 'manifest.json'
    path.write_text(json.dumps(value))
    return path, value


def maps_for(value):
    lines = []
    for binary in value['binaries'].values():
        path = Path(binary['path'])
        st = path.stat()
        dev = f'{os.major(st.st_dev):02x}:{os.minor(st.st_dev):02x}'
        lines.append(f'00010000-00020000 r-xp 00000000 {dev} {st.st_ino} {path}')
    return '\n'.join(lines)


def test_explicit_complete_build_manifest_accepted(build_manifest):
    path, value = build_manifest
    assert p.validate_manifest(path, p.file_hash(path)) == value


@pytest.mark.parametrize('bad_hash', ['', None, '0' * 64])
def test_manifest_pin_required(build_manifest, bad_hash):
    path, _ = build_manifest
    with pytest.raises(p.ProbeError, match='manifest_sha256_required|manifest_hash_mismatch'):
        p.validate_manifest(path, bad_hash)


@pytest.mark.parametrize('field,bad', [('status', 'failed'), ('vendor_version', '2.6.11'),
                                       ('observation_mode', 'blackbox'), ('schema', 'anything')])
def test_wrong_build_contract_rejected(build_manifest, field, bad):
    path, value = build_manifest
    value[field] = bad
    path.write_text(json.dumps(value))
    with pytest.raises(p.ProbeError, match='unsupported_or_incomplete'):
        p.validate_manifest(path, p.file_hash(path))


def test_changed_provider_source_not_accepted(build_manifest):
    path, value = build_manifest
    value['provider_source_sha256'] = '0' * 64
    path.write_text(json.dumps(value))
    with pytest.raises(p.ProbeError, match='provider_source_identity_mismatch'):
        p.validate_manifest(path, p.file_hash(path))


@pytest.mark.parametrize('role', ['provider', 'vendor'])
def test_replaced_binary_not_accepted(build_manifest, role):
    path, value = build_manifest
    Path(value['binaries'][role]['path']).write_bytes(b'replaced')
    with pytest.raises(p.ProbeError, match='binary_hash_mismatch:' + role):
        p.validate_manifest(path, p.file_hash(path))


def test_native_result_layout_exact():
    assert ctypes.sizeof(p.Result) == 64
    assert ctypes.alignment(p.Result) == 8
    assert {name: getattr(p.Result, name).offset for name in p.EXPECTED_OFFSETS} == p.EXPECTED_OFFSETS


def test_maps_require_exact_path_device_inode(build_manifest):
    _, value = build_manifest
    rows = p.verify_mapped_binaries(value, maps_for(value))
    assert {row['role'] for row in rows} == {'vendor', 'provider'}


@pytest.mark.parametrize('damage', ['path', 'inode', 'device', 'deleted', 'missing', 'second_vendor'])
def test_wrong_native_mapping_rejected(build_manifest, damage):
    _, value = build_manifest
    text = maps_for(value)
    fields = text.splitlines()[0].split(maxsplit=5)
    if damage == 'path':
        fields[5] = '/opt/ros/jazzy/lib/libfastrtps.so.2.14.7'
    elif damage == 'inode':
        fields[4] = str(int(fields[4]) + 1)
    elif damage == 'device':
        fields[3] = '00:00'
    elif damage == 'deleted':
        fields[5] += ' (deleted)'
    if damage == 'missing':
        text = text.splitlines()[0]
    elif damage == 'second_vendor':
        text += '\n' + text.splitlines()[0].replace(value['binaries']['vendor']['path'], '/elsewhere/libfastrtps.so.2.14.7')
    else:
        text = ' '.join(fields) + '\n' + text.splitlines()[1]
    with pytest.raises(p.ProbeError, match='mapped_binary_identity_mismatch|required_binary_not_mapped'):
        p.verify_mapped_binaries(value, text)


def test_no_assert_optimization_bypass():
    with pytest.raises(p.ProbeError, match='failed-native-check'):
        p._require(False, 'failed-native-check')


@pytest.mark.parametrize('value', [None, [], {'schema': 'anything'}, {'verification': []}])
def test_non_object_or_incomplete_json_normalized_to_probe_error(tmp_path, value):
    path = tmp_path / 'invalid.json'
    path.write_text(json.dumps(value))
    with pytest.raises(p.ProbeError):
        p.validate_manifest(path, p.file_hash(path))
