"""Optional local-API build qualification through file and native-loader seams.

All binary/process data here are labelled fixture bytes. No compiler, native
library, ROS context or DDS entity is created, and no execution credit follows.
"""
from copy import deepcopy
import json
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import pytest

from test_fastdds_peer_probe import build_manifest
from r2f.instrumentation.fastdds_peer import local_api, probe
from r2f.runtime import fastdds_peer_observer as observer


def extension_manifest(build_manifest):
    path, value = build_manifest
    attempt = path.parent / 'attempt'
    attempt.mkdir()
    snapshot = attempt / 'local_api.cpp'
    snapshot.write_bytes(local_api.source_path().read_bytes())
    provider = value['binaries']['provider']['path']
    value.update(attempt=str(attempt), local_api=local_api.build_receipt(snapshot, provider),
                 commands=[{'name': 'provider-build', 'returncode': 0,
                            'argv': ['labelled-compiler-double', '-shared', '-fPIC',
                                     str(snapshot), '-o', provider]}])
    path.write_text(json.dumps(value))
    return path, value


def test_explicit_extension_is_file_qualified_without_a_compiler_or_native_load(build_manifest):
    path, value = extension_manifest(build_manifest)
    with patch('ctypes.CDLL', side_effect=AssertionError('native load forbidden')), \
            patch('subprocess.run', side_effect=AssertionError('compiler forbidden')):
        result = probe.validate_manifest(path, probe.file_hash(path))
    assert result['local_api'] == value['local_api']


@pytest.mark.parametrize('damage', ['source_hash', 'snapshot', 'provider', 'missing_command',
                                   'failed_command', 'bool_returncode', 'other_output', 'outside_attempt'])
def test_extension_identity_or_compile_gaps_never_upgrade_a_peer_build(build_manifest, damage):
    path, value = extension_manifest(build_manifest)
    if damage == 'source_hash':
        value['local_api']['source_sha256'] = '0' * 64
    elif damage == 'snapshot':
        Path(value['local_api']['source_snapshot']).write_bytes(b'labelled wrong source')
    elif damage == 'provider':
        value['local_api']['provider_sha256'] = '0' * 64
    elif damage == 'missing_command':
        value['commands'] = []
    elif damage == 'failed_command':
        value['commands'][0]['returncode'] = 1
    elif damage == 'bool_returncode':
        value['commands'][0]['returncode'] = False
    elif damage == 'other_output':
        value['commands'][0]['argv'][-1] = '/labelled-other-provider'
    else:
        value['attempt'] = str(path.parent / 'another-attempt')
    path.write_text(json.dumps(value))
    with pytest.raises(probe.ProbeError, match='local_api'):
        probe.validate_manifest(path, probe.file_hash(path))


def test_legacy_peer_binding_cannot_call_unqualified_optional_api(build_manifest):
    path, _ = build_manifest
    binding = object.__new__(observer._Bindings)
    binding.reference = {'observation_mode': 'instrumentation',
                         'manifest_path': str(path), 'manifest_sha256': probe.file_hash(path)}
    binding.libs = {'provider': SimpleNamespace(r2f_fastdds_local_v1=lambda: None)}
    with pytest.raises(ValueError, match='local_api_build_receipt_missing'):
        binding.local_api()


def test_explicit_binding_rechecks_companion_source_before_returning_function(build_manifest):
    path, value = extension_manifest(build_manifest)
    binding = object.__new__(observer._Bindings)
    binding.reference = {'observation_mode': 'instrumentation',
                         'manifest_path': str(path), 'manifest_sha256': probe.file_hash(path)}
    function = lambda: None
    binding.libs = {'provider': SimpleNamespace(r2f_fastdds_local_v1=function)}
    assert binding.local_api() is function
    Path(value['local_api']['source_snapshot']).write_bytes(b'labelled source drift')
    with pytest.raises(ValueError, match='local_api'):
        binding.local_api()
