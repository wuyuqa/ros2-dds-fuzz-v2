"""Pinned loader selection, with authored files only; never load a native DSO."""
from copy import deepcopy
import hashlib
import json
import os
from pathlib import Path
from unittest.mock import patch

import pytest

from test_fastdds_peer_probe import build_manifest
from r2f.instrumentation.fastdds_peer import activation as peer
from r2f.instrumentation.fastdds_filter_trace import activation as trace


def save(path, data):
    path.write_text(json.dumps(data))
    return dict(manifest_path=str(path), manifest_sha256=trace._hash(path))


@pytest.fixture
def selected(build_manifest, tmp_path):
    original_path, original = build_manifest
    install = tmp_path / 'trace-install'; install.mkdir()
    binary = install / 'libr2f_fastdds_filter_trace.so'
    binary.write_bytes(b'authored trace binary, not loadable')
    value = dict(schema=trace.SCHEMA, status='built', observation_mode='instrumentation',
        verification={'compilation':'passed'}, abi=dict(trace.ABI), install=str(install),
        binary=dict(path=str(binary),sha256=trace._hash(binary)),
        vendor_binary=deepcopy(original['binaries']['vendor']),
        hook_source_sha256=trace._hash(Path(trace.__file__).with_name('hook.cpp')),
        builder_source_sha256=trace._hash(Path(trace.__file__).with_name('build.py')),
        peer_manifest_reference=dict(manifest_path=str(original_path), manifest_sha256=trace._hash(original_path)))
    path = tmp_path/'trace.json';ref=save(path,value)
    extended=deepcopy(original);extended['filter_trace']=ref
    parent = tmp_path/'extended.json';parent_ref=save(parent,extended)
    return value,path,extended,dict(observation_mode='instrumentation',**parent_ref)


def maps_for(binary):
    p=Path(binary['path']);st=p.stat();device=f'{os.major(st.st_dev):02x}:{os.minor(st.st_dev):02x}'
    return f'00010000-00020000 r-xp 00000000 {device} {st.st_ino} {p}'


def test_optional_trace_activation_is_pinned_and_import_only(selected):
    value,_,extended,ref=selected
    with patch('ctypes.CDLL',side_effect=AssertionError('NO NATIVE LOAD')):
        assert peer.manifest_for_reference(ref)==extended
        env=peer.environment_for_reference({'LD_LIBRARY_PATH':'/opt/ros/jazzy/lib'},ref)
        assert env['LD_PRELOAD']==value['binary']['path']
        trace.verify_loader_selection(extended,environment=env,maps_text=maps_for(value['binary']))
    original={'LD_LIBRARY_PATH':'/opt/ros/jazzy/lib'}
    assert peer.environment_for_reference(original,None)==original
    assert 'LD_PRELOAD' not in original


@pytest.mark.parametrize('damage', ['binary','source','abi','vendor','status','manifest_hash','original_peer'])
def test_broken_optional_trace_cannot_activate(selected,damage):
    value,path,extended,_=selected
    if damage=='binary': Path(value['binary']['path']).write_bytes(b'changed')
    elif damage=='source': value['hook_source_sha256']='0'*64
    elif damage=='abi': value['abi']['record_size']=0
    elif damage=='vendor': value['vendor_binary']['sha256']='0'*64
    elif damage=='status': value['status']='failed'
    elif damage=='original_peer': value['peer_manifest_reference']['manifest_sha256']='0'*64
    if damage not in {'binary','manifest_hash'}:extended['filter_trace']=save(path,value)
    elif damage=='manifest_hash': extended['filter_trace']['manifest_sha256']='0'*64
    with pytest.raises(ValueError): trace.selected_manifest(extended)


@pytest.mark.parametrize('damage', ['missing','extra_preload','audit','mapping','deleted','inode'])
def test_selected_trace_does_not_waive_loader_or_mapping_gates(selected,damage):
    value,_,extended,_=selected
    env={'LD_PRELOAD':value['binary']['path']};maps=maps_for(value['binary'])
    if damage=='missing': env.clear()
    elif damage=='extra_preload': env['LD_PRELOAD']+=':/foreign.so'
    elif damage=='audit':env['LD_AUDIT']='/foreign.so'
    elif damage=='mapping':maps=maps.replace(value['binary']['path'],'/foreign/libr2f_fastdds_filter_trace.so')
    elif damage=='deleted':maps+=' (deleted)'
    else:
        fields=maps.split();fields[4]=str(int(fields[4])+1);maps=' '.join(fields)
    with pytest.raises(ValueError):trace.verify_loader_selection(extended,environment=env,maps_text=maps)
