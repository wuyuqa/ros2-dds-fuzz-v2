"""Explicit mode/loader selection without starting ROS or loading a native DSO."""
from copy import deepcopy
import json
from pathlib import Path
from unittest.mock import patch

import pytest

from test_fastdds_peer_probe import build_manifest, maps_for
from r2f.instrumentation.fastdds_peer import activation as a
from r2f.instrumentation.fastdds_peer.probe import file_hash, ProbeError


def reference(path):
    return dict(observation_mode='instrumentation', manifest_path=str(path), manifest_sha256=file_hash(path))


def config(path):
    ref = reference(path)
    return {'rmw': 'rmw_fastrtps_cpp', 'observation_mode': 'instrumentation',
            'fastdds_peer_instrumentation': {k: ref[k] for k in ('manifest_path', 'manifest_sha256')}}


def test_default_blackbox_has_no_override():
    assert a.reference_from_config({}) is None
    original = {'LD_LIBRARY_PATH': '/opt/ros/jazzy/lib', 'PATH': '/usr/bin'}
    assert a.environment_for_reference(original, None) == original


def test_explicit_manifest_checked_without_native_load(build_manifest):
    path, value = build_manifest
    with patch('ctypes.CDLL', side_effect=AssertionError('NO NATIVE LOAD')):
        ref = a.reference_from_config(config(path))
        assert ref == reference(path)
        assert a.manifest_for_reference(ref) == value
        original = {'LD_LIBRARY_PATH': '/opt/ros/jazzy/lib'}
        result = a.environment_for_reference(original, ref)
        assert result['LD_LIBRARY_PATH'] == str(Path(value['install']) / 'lib') + ':/opt/ros/jazzy/lib'
        assert original == {'LD_LIBRARY_PATH': '/opt/ros/jazzy/lib'}


@pytest.mark.parametrize('change', ['blackbox', 'missing', 'cyclone', 'mixed', 'bad_hash', 'extra', 'relative'])
def test_unbound_or_wrong_mode_cannot_activate(build_manifest, change):
    path, _ = build_manifest
    cfg = config(path)
    if change == 'blackbox': cfg['observation_mode'] = 'blackbox'
    elif change == 'missing': cfg.pop('fastdds_peer_instrumentation')
    elif change == 'cyclone': cfg['rmw'] = 'rmw_cyclonedds_cpp'
    elif change == 'mixed': cfg['rmws'] = {'reader': 'rmw_cyclonedds_cpp'}
    elif change == 'bad_hash': cfg['fastdds_peer_instrumentation']['manifest_sha256'] = '0' * 64
    elif change == 'extra': cfg['fastdds_peer_instrumentation']['skip_verification'] = True
    elif change == 'relative': cfg['fastdds_peer_instrumentation']['manifest_path'] = 'manifest.json'
    with pytest.raises((ProbeError, OSError)):
        a.reference_from_config(cfg)


def test_selected_loaded_vendor_requires_exact_path_inode_hash(build_manifest):
    path, value = build_manifest
    row = a.verify_vendor_mapping(reference(path), maps_text=maps_for(value))
    assert row['identity_verified'] and not row['membership_verified']
    assert row['manifest_reference'] == reference(path)
    with pytest.raises(ProbeError, match='selected_vendor_mapping_mismatch'):
        a.verify_vendor_mapping(reference(path), maps_text=maps_for(value).replace(
            value['binaries']['vendor']['path'], '/foreign/libfastrtps.so.2.14.7'))


def test_blackbox_rejects_instrumented_vendor_even_with_same_soname(build_manifest, monkeypatch):
    _, value = build_manifest
    from tempfile import TemporaryDirectory
    # Pin another real file as the authored "installed" vendor for this test.
    installed = Path(value['install']).parent / 'installed/libfastrtps.so.2.14.7'
    installed.parent.mkdir()
    installed.write_bytes(b'original fixture')
    monkeypatch.setattr(a, 'ORIGINAL_VENDOR', installed)
    monkeypatch.setattr(a, 'ORIGINAL_VENDOR_SHA256', file_hash(installed))
    with pytest.raises(ProbeError, match='selected_vendor_mapping_mismatch'):
        a.verify_vendor_mapping(None, maps_text=maps_for(value))


def test_unknown_loader_hooks_not_accepted(build_manifest):
    path, _ = build_manifest
    with pytest.raises(ProbeError, match='untracked_loader_hook'):
        a.environment_for_reference({'LD_PRELOAD': '/foreign.so'}, reference(path))
