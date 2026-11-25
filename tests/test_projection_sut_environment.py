"""Explicit SUT builds at the canonical input and external environment seams.

All dependency bytes below are fixtures and are never loaded as native code.
"""
import hashlib
import json
from pathlib import Path

import pytest

from r2f.campaign.runner import run_campaign


TARGET = 'builtin.discovery_projection_reconciliation'
KEY = 'projection_sut_environment'


def test_canonical_plan_keeps_sut_build_reference_without_claiming_execution(tmp_path):
    ref = {'path': '/offline/qualified-sut.json', 'sha256': 'a' * 64}
    report = run_campaign(tmp_path / 'plan', repo=Path(__file__).resolve().parents[1],
        run_id='offline-explicit-sut', domain_id=224, target_id=TARGET,
        rmws=['rmw_cyclonedds_cpp'], parameters={
            'projection_discovery_layers': 'participant_topic_v1', KEY: ref})
    assert report['maturity_denominator_changed'] is False
    assert len(report['rows']) == 12
    for row in report['rows']:
        config = json.loads(Path(row['runtime_config_path']).read_text())
        capsule = json.loads(Path(row['replay_path']).read_text())
        assert config[KEY] == ref
        assert config['final_input'][KEY] == ref
        assert ref == capsule['input']['effective_input']['final_input'][KEY]
        assert row['execution_stage'] == 'planned'
        assert not row.get('actual_run')


def fixture_sut(tmp_path):
    roots = {name: tmp_path / name for name in ('rmw', 'core', 'rmw-source', 'core-source')}
    paths = [roots['rmw'] / 'lib/librmw_cyclonedds_cpp.so',
             roots['rmw'] / 'share/rmw_cyclonedds_cpp/package.xml',
             roots['rmw'] / 'share/ament_index/resource_index/packages/rmw_cyclonedds_cpp',
             roots['core'] / 'lib/libddsc.so.11',
             roots['core'] / 'include/dds/features.h',
             roots['rmw-source'] / 'source.cpp', roots['core-source'] / 'source.c',
             tmp_path / 'build.json']
    for path in paths:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(b'offline fixture; never executed or loaded')
    digest = lambda path: hashlib.sha256(path.read_bytes()).hexdigest()
    value = {'schema': 'r2f.projection-sut-environment/v1',
        'base_dependency_lock_sha256': digest(Path(__file__).resolve().parents[1] / 'config/dependencies.lock.json'),
        'rmw': {'prefix': str(roots['rmw']), 'version': 'fixture',
                'library': str(paths[0]), 'library_sha256': digest(paths[0]),
                'package_xml_sha256': digest(paths[1])},
        'core': {'prefix': str(roots['core']), 'version': '11.0.1',
                 'library': str(paths[3]), 'library_sha256': digest(paths[3])},
        'source': [{'root': str(roots[k]), 'origin': 'fixture_not_execution',
                    'upstream_commit': 'a' * 40} for k in ('rmw-source', 'core-source')],
        'build': {'kind': 'backport_sut', 'instrumented': False,
                  'record': {'path': str(paths[-1]), 'sha256': digest(paths[-1])}},
        'files': {str(path): digest(path) for path in paths}}
    path = tmp_path / 'sut.json'
    path.write_text(json.dumps(value))
    return {'path': str(path), 'sha256': digest(path)}, value


def test_sut_preflight_preserves_lock_and_detects_library_or_inventory_drift(tmp_path):
    from r2f.runtime.projection_sut import load_sut_environment
    ref, value = fixture_sut(tmp_path)
    assert load_sut_environment(ref) == value
    extra = Path(value['core']['prefix']) / 'lib/libshadow.so'
    extra.write_bytes(b'unqualified')
    with pytest.raises(ValueError, match='inventory_changed'):
        load_sut_environment(ref)
    extra.unlink()
    Path(value['rmw']['library']).write_bytes(b'changed after qualification')
    with pytest.raises(ValueError, match='dependency_changed'):
        load_sut_environment(ref)


def test_environment_activation_uses_only_explicit_build_and_verifies_mapped_core(tmp_path, monkeypatch):
    from r2f.runtime.environment import activated_environment, LOCK_FILE, REPO
    import subprocess
    from types import SimpleNamespace
    from copy import deepcopy
    ref, value = fixture_sut(tmp_path)
    packages = json.loads(LOCK_FILE.read_text())['packages']
    packages['rmw_cyclonedds_cpp'] = dict(value['rmw'], available=True)
    payload = {'ros_distro': 'jazzy', 'packages': packages,
        'rmw': {name: record for name, record in packages.items() if name.startswith('rmw_')},
        'core_libraries': [{'path': value['core']['library'], 'sha256': value['core']['library_sha256']}],
        'activation_environment': {'R2F_OVERLAY_KIND': 'python_source_overlay',
                                 'R2F_OVERLAY_SOURCE': str(REPO)}}
    commands = []
    def fake_external_probe(command, **kwargs):
        commands.append(command)
        return SimpleNamespace(returncode=0, stdout=json.dumps(deepcopy(payload)))
    monkeypatch.setattr(subprocess, 'run', fake_external_probe)
    config = {'target_id': TARGET, 'rmw': 'rmw_cyclonedds_cpp',
              'projection_discovery_layers': 'participant_topic_v1', KEY: ref}
    result, _ = activated_environment(config)
    assert result['sut_environment']['manifest'] == ref
    assert result['sut_environment']['core'] == value['core']
    assert ':'.join(value[k]['prefix'] for k in ('rmw', 'core')) in commands[0]
    assert ':'.join(value[k]['prefix'] + '/lib' for k in ('rmw', 'core')) in commands[0]
    payload['core_libraries'][0]['sha256'] = '0' * 64
    with pytest.raises(RuntimeError, match='loaded_core_mismatch'):
        activated_environment(config)


def test_sut_carrier_requires_actual_selected_libraries_not_just_a_manifest(tmp_path):
    from r2f.runtime.projection_sut import load_sut_environment, verify_sut_libraries
    ref, value = fixture_sut(tmp_path)
    sut = load_sut_environment(ref)
    loaded = [{'path': value[name]['library'], 'sha256': value[name]['library_sha256']}
              for name in ('rmw', 'core')]
    verify_sut_libraries(sut, loaded)
    with pytest.raises(ValueError, match='worker_library_mismatch'):
        verify_sut_libraries(sut, loaded[:1])
    with pytest.raises(ValueError, match='worker_library_mismatch'):
        verify_sut_libraries(sut, [*loaded, {'path': '/opt/old/libddsc.so.0', 'sha256': 'a' * 64}])


@pytest.mark.parametrize('change', [
    {'projection_discovery_layers': None}, {'rmw': 'rmw_fastrtps_cpp'},
    {'target_id': 'discovery.late_endpoint'}, {'rmws': {'reader': 'rmw_fastrtps_cpp'}},
])
def test_sut_reference_cannot_escape_selected_cyclone_carrier(change):
    parameters = {'projection_discovery_layers': 'participant_topic_v1',
                  KEY: {'path': '/offline/sut.json', 'sha256': 'a' * 64}}
    config = {'target_id': TARGET, 'rmw': 'rmw_cyclonedds_cpp', **parameters, **change}
    from r2f.runtime.projection_sut import validate_sut_reference
    with pytest.raises(ValueError, match='projection_sut_requires'):
        validate_sut_reference(config)


def test_sut_manifest_cannot_replace_the_underlay_lock(tmp_path):
    from r2f.runtime.projection_sut import load_sut_environment
    ref, value = fixture_sut(tmp_path)
    value['base_dependency_lock_sha256'] = '0' * 64
    Path(ref['path']).write_text(json.dumps(value))
    ref['sha256'] = hashlib.sha256(Path(ref['path']).read_bytes()).hexdigest()
    with pytest.raises(ValueError, match='manifest_incomplete'):
        load_sut_environment(ref)


def test_binding_loader_is_pinned_independently_of_sut_paths(tmp_path):
    from r2f.runtime.discovery_tools import tools_process_environment
    library = tmp_path / 'observer/lib/libddsc.so'
    manifest = {'library': {'path': str(library)}}
    original = {'LD_LIBRARY_PATH': '/sut/lib:/ros/lib', 'CYCLONEDDS_URI': 'observer.xml'}
    child = tools_process_environment(manifest, original)
    assert child['LD_LIBRARY_PATH'] == str(library.parent) + ':/sut/lib:/ros/lib'
    assert child['CYCLONEDDS_URI'] == 'observer.xml'
    assert original['LD_LIBRARY_PATH'] == '/sut/lib:/ros/lib'


def test_explicit_sut_ownership_observer_uses_mapped_native_abi_without_creating_dds(tmp_path):
    """Optional actual dependency eligibility probe; no DDS context or entities."""
    import os
    import subprocess
    import sys
    reference = os.environ.get('R2F_TEST_PROJECTION_SUT')
    if not reference:
        pytest.skip('explicit independently built SUT manifest not selected')
    code = """
import ctypes, json, os, sys
from r2f.runtime.projection_sut import load_sut_environment
from r2f.runtime.topology_worker import CycloneMatchedPeerObserver
ref = json.load(open(sys.argv[1]))
sut = load_sut_environment(ref)
library = ctypes.CDLL(sut['core']['library'], mode=os.RTLD_LOCAL)
try:
    CycloneMatchedPeerObserver()
except ValueError:
    pass
else:
    raise AssertionError('unselected mapped core was accepted')
observer = CycloneMatchedPeerObserver(sut_reference=ref)
p = observer.provenance
assert p['creates_entities'] is False
assert p['library_sha256'] == sut['core']['library_sha256']
assert p['abi'] == 'CycloneDDS-11.0.1-lp64-endpoint64'
assert list(p['c_probe_layout']) == [64, 0, 16, 32, 40, 48, 56]
print(json.dumps({'abi': p['abi'], 'creates_entities': p['creates_entities']}))
"""
    child = subprocess.run([sys.executable, '-c', code, reference], text=True,
        capture_output=True, check=False, timeout=20)
    assert child.returncode == 0, child.stderr
    assert json.loads(child.stdout)['creates_entities'] is False
