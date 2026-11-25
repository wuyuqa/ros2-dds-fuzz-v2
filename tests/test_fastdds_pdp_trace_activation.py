"""Explicit PDP observation is a pinned carrier boundary, never an inferred mode."""
from copy import deepcopy
import hashlib
import json

import pytest

from test_fastdds_peer_probe import build_manifest
from r2f.instrumentation.fastdds_peer.activation import manifest_for_reference


def test_selected_pdp_requires_actual_pinned_observer(build_manifest, tmp_path):
    path, original = build_manifest
    selected = deepcopy(original)
    selected['pdp_trace'] = {'manifest_path': str(tmp_path / 'missing-pdp.json'),
                             'manifest_sha256': '0' * 64}
    path.write_text(json.dumps(selected))
    ref = dict(observation_mode='instrumentation', manifest_path=str(path),
               manifest_sha256=hashlib.sha256(path.read_bytes()).hexdigest())
    with pytest.raises((ValueError, OSError)):
        manifest_for_reference(ref)


def test_pdp_on_off_pin_same_binary_and_control_without_loading(build_manifest, tmp_path):
    from pathlib import Path
    from unittest.mock import patch
    from r2f.instrumentation.fastdds_pdp_trace import activation as pdp
    from r2f.instrumentation.fastdds_peer.activation import environment_for_reference
    base_path,base=build_manifest
    install=tmp_path/'trace-install';install.mkdir()
    binary=install/'libr2f_fastdds_pdp_trace.so';binary.write_bytes(b'not a native library')
    record=dict(schema=pdp.SCHEMA,status='built',observation_mode='instrumentation',abi=pdp.ABI,
        verification={'compilation':'passed'},capture_enabled=True,install=str(install),
        binary={'path':str(binary),'sha256':pdp.digest(binary)},vendor_binary=base['binaries']['vendor'],
        peer_manifest_reference={'manifest_path':str(base_path),'manifest_sha256':pdp.digest(base_path)},
        hook_source_sha256=pdp.digest(Path(pdp.__file__).with_name('hook.cpp')),
        builder_source_sha256=pdp.digest(Path(pdp.__file__).with_name('build.py')))
    preloads=[]
    for enabled in (True,False):
        record['capture_enabled']=enabled
        trace=tmp_path/f'trace-{enabled}.json';trace.write_text(json.dumps(record))
        selected=dict(base,pdp_trace={'manifest_path':str(trace),'manifest_sha256':pdp.digest(trace)})
        path=tmp_path/f'peer-{enabled}.json';path.write_text(json.dumps(selected))
        ref={'observation_mode':'instrumentation','manifest_path':str(path),'manifest_sha256':pdp.digest(path)}
        with patch('ctypes.CDLL',side_effect=AssertionError('NO NATIVE LOAD')):
            assert manifest_for_reference(ref)==selected
            env=environment_for_reference({'LD_LIBRARY_PATH':'/opt/ros/jazzy/lib'},ref)
            preloads.append(env['LD_PRELOAD'])
            assert env['R2F_FASTDDS_PDP_TRACE_ENABLED']==('1' if enabled else '0')
    assert preloads==[str(binary)]*2
