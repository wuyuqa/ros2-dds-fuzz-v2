"""Public carrier qualification, offline fixtures only; never loads native code."""
from copy import deepcopy
import hashlib
import json
import os
from pathlib import Path

import pytest

from r2f.instrumentation.compatibility import qualify_carrier
from r2f.instrumentation.fastdds_peer import probe as peer


@pytest.mark.parametrize('vendor,version,carrier', [
    ('cyclonedds', '0.10.5', 'ros'), ('fastdds', '2.14.7', 'native'),
    ('cyclonedds', '11.0.0', 'binding'),
])
def test_version_label_cannot_qualify_a_carrier(vendor, version, carrier):
    result = qualify_carrier(vendor=vendor, version=version,
                             observation_mode='instrumentation', carrier=carrier)
    assert result['status'] == 'unqualified'
    assert result['run_eligible'] is False
    assert result['observer_adaptation']['status'] == 'unverified'
    assert 'probe_reference_missing' in result['issues']
    assert result['execution_evidence'] is False


def reference(path):
    return {'path': str(path), 'sha256': hashlib.sha256(path.read_bytes()).hexdigest()}


def write_json(path, value):
    path.write_text(json.dumps(value, sort_keys=True))
    return reference(path)


@pytest.fixture(scope='module')
def retained_source_receipts():
    """Read-only historical input; no compiler, loader or live process inspection.

    Set R2F_FASTDDS_CP1_RETAINED_PROBE to relocate the original evidence bundle.
    These tests explicitly skip if the retained source evidence is unavailable.
    """
    default = (Path.home() / 'ros2-dds-fuzz-v2/results/main-review/'
               's1-fastdds-peer-build-20261120/native-no-dds-probe-v4.json')
    path = Path(os.environ.get('R2F_FASTDDS_CP1_RETAINED_PROBE', default))
    if not path.is_file():
        pytest.skip('CP1 retained source/archive fixture unavailable: ' + str(path))
    probe = json.loads(path.read_bytes())
    build_path = Path(probe['build_manifest'])
    assert reference(build_path)['sha256'] == probe['build_manifest_sha256']
    build = json.loads(build_path.read_bytes())
    assert reference(Path(build['archive']))['sha256'] == build['archive_sha256']
    return {key: build[key] for key in ('archive', 'archive_sha256', 'source_frozen',
                                       'source_before', 'source_after', 'patch')}


@pytest.fixture
def retained_carrier(tmp_path, retained_source_receipts):
    """Synthetic binaries/processes; retained, pinned source receipts stay read-only."""
    install = tmp_path / 'install'
    install.mkdir()
    binaries = {}
    mapped = []
    for role in ('vendor', 'provider'):
        path = install / (role + '.so')
        path.write_bytes(('offline fixture ' + role).encode())
        binaries[role] = reference(path)
        stat = path.stat()
        mapped.append({**binaries[role], 'role': role, 'inode': stat.st_ino,
                       'device': f'{os.major(stat.st_dev):02x}:{os.minor(stat.st_dev):02x}'})
    build_root = tmp_path / 'build'
    build_root.mkdir()
    for name in ('compile_commands.json', 'CMakeCache.txt'):
        (build_root / name).write_text('offline fixture')
    source_receipts = deepcopy(retained_source_receipts)
    source_receipts['patch']['source_root'] = str(tmp_path / 'source')
    build = {'schema': peer.BUILD_SCHEMA, 'status': 'built',
             'observation_mode': 'instrumentation', 'vendor_version': '2.14.7',
             'verification': {'compilation': 'passed'}, 'install': str(install),
             'source': str(tmp_path / 'source'), **source_receipts,
             'build': str(build_root), 'binaries': binaries,
             'commands': [{'name': name, 'argv': ['offline-fixture', name], 'returncode': 0}
                          for name in ('configure', 'vendor-build', 'vendor-install', 'provider-build')],
             'process': {'pid': 11, 'pgid': 11, 'sid': 10, 'start_ticks': 99,
                         'dedicated_process_group': True},
             'provider_source_sha256': peer.file_hash(Path(peer.__file__).with_name('provider.cpp'))}
    build['compile_commands_sha256'] = reference(build_root / 'compile_commands.json')['sha256']
    build['cmake_cache_sha256'] = reference(build_root / 'CMakeCache.txt')['sha256']
    build_ref = write_json(tmp_path / 'build-manifest.json', build)
    probe = {'schema': 'r2f.fastdds-peer-no-dds-probe/v1', 'status': 'passed',
             'build_manifest': build_ref['path'], 'build_manifest_sha256': build_ref['sha256'],
             'mapped_binaries': mapped, 'pid': 17, 'pgid': 17, 'sid': 17,
             'process_start_ticks': 123, 'dedicated_process_group': True,
             'dds_entities_created': False, 'execution_evidence': False,
             'result_size': 64, 'result_offsets': dict(peer.EXPECTED_OFFSETS),
             'checks': [{'function': name, 'null_endpoint': 'rejected', 'null_output': 'rejected',
                         'abi_mismatch': 'rejected_without_write'}
                        for name in ('r2f_fastdds_peer_writer_v1', 'r2f_fastdds_peer_reader_v1')]}
    probe_ref = write_json(tmp_path / 'probe.json', probe)
    dep = tmp_path / 'libfastcdr.so'
    dep.write_bytes(b'offline dependency')
    args = dict(vendor='fastdds', version='2.14.7', carrier='native',
                observation_mode='instrumentation', build_reference=build_ref,
                probe_reference=probe_ref, dependencies=[reference(dep)])
    return args, build, probe


def test_consumer_gets_separate_pinned_identities_without_live_admission(retained_carrier):
    args, build, probe = retained_carrier
    result = qualify_carrier(**args)
    assert result['status'] == 'artifact_qualified'
    assert result['issues'] == []
    assert result['observer_adaptation']['status'] == 'abi_probe_verified'
    assert result['identities']['source'] == build['source_frozen']
    assert result['identities']['build'] == args['build_reference']
    assert result['identities']['dependencies'] == args['dependencies']
    assert result['identities']['binaries'] == build['binaries']
    assert result['identities']['patch']['patch_id'] == 'r2f-fastdds-peer-friend-v1'
    assert result['identities']['process']['process_start_ticks'] == 123
    assert result['identities']['probe'] == args['probe_reference']
    assert result['run_eligible'] is False
    assert result['live_compatibility'] == 'not_evaluated'


@pytest.mark.parametrize('change,issue', [
    ('api', 'observer_api_incompatible_or_unverified'),
    ('process', 'probe_process_identity_incomplete'),
    ('mapping', 'probe_binary_identity_mismatch'),
    ('build_binding', 'probe_build_binding_or_scope_invalid'),
    ('schema', 'probe_build_binding_or_scope_invalid'),
    ('dds_scope', 'probe_build_binding_or_scope_invalid'),
])
def test_incompatible_or_incomplete_probe_is_not_qualified(retained_carrier, change, issue):
    args, _, probe = retained_carrier
    if change == 'api':
        probe['result_size'] = 32
    elif change == 'process':
        del probe['process_start_ticks']
    elif change == 'mapping':
        probe['mapped_binaries'][0]['sha256'] = '0' * 64
    elif change == 'build_binding':
        probe['build_manifest_sha256'] = '0' * 64
    elif change == 'schema':
        probe['schema'] = 'unknown/v1'
    else:
        probe['dds_entities_created'] = True
    args['probe_reference'] = write_json(Path(args['probe_reference']['path']), probe)
    result = qualify_carrier(**args)
    assert result['status'] == 'unqualified'
    assert issue in result['issues']
    assert result['run_eligible'] is False


@pytest.mark.parametrize('selection', [
    {'observation_mode': 'blackbox'}, {'carrier': 'ros'}, {'carrier': 'binding'},
    {'version': '3.2.4'}, {'version': '11.0.0'},
])
def test_other_modes_and_researched_versions_do_not_borrow_qualification(retained_carrier, selection):
    args, _, _ = retained_carrier
    result = qualify_carrier(**{**args, **selection})
    assert result['status'] == 'unqualified'
    assert result['run_eligible'] is False
    assert result['issues']
    assert result['observer_adaptation']['status'] == 'unverified'


@pytest.mark.parametrize('component', ['build_reference', 'probe_reference', 'dependency'])
def test_artifact_byte_changes_reject_old_reference(retained_carrier, component):
    args, _, _ = retained_carrier
    ref = args['dependencies'][0] if component == 'dependency' else args[component]
    Path(ref['path']).write_bytes(b'changed after pinning')
    result = qualify_carrier(**args)
    assert result['status'] == 'unqualified'
    assert any('artifact_hash_mismatch' in issue for issue in result['issues'])


def test_missing_dependency_still_exposes_available_build_and_process_identity(retained_carrier):
    args, _, _ = retained_carrier
    args['dependencies'] = []
    result = qualify_carrier(**args)
    assert result['status'] == 'unqualified'
    assert 'dependency_identity_unavailable' in result['issues']
    assert result['identities']['build'] == args['build_reference']
    assert result['identities']['process']['pid'] == 17


def test_build_failure_is_retained_and_not_replaced_by_version_label(retained_carrier):
    args, build, _ = retained_carrier
    build['status'] = 'failed'
    args['build_reference'] = write_json(Path(args['build_reference']['path']), build)
    result = qualify_carrier(**args)
    assert result['status'] == 'unqualified'
    assert result['identities']['build_status'] == 'failed'
    assert any('unsupported_or_incomplete_build_manifest' in issue for issue in result['issues'])


def test_evidence_and_caller_selection_are_not_modified(retained_carrier):
    from copy import deepcopy
    args, _, _ = retained_carrier
    before = deepcopy(args)
    files = [Path(args[key]['path']) for key in ('build_reference', 'probe_reference')]
    raw = [path.read_bytes() for path in files]
    result = qualify_carrier(**args)
    result['identities']['dependencies'][0]['sha256'] = '0' * 64
    assert args == before
    assert [path.read_bytes() for path in files] == raw


def test_cyclone_config_and_loader_success_do_not_prove_observer_adaptation(tmp_path):
    header = tmp_path / 'version.h'
    header.write_text('#define DDS_VERSION "0.10.5"')
    config_header = tmp_path / 'config.h'
    config_header.write_text('discovery plist OutputFile AppendToFile')
    probe = {'schema': 'r2f.cyclonedds-trace/v1', 'scope': 'loader_probe_only',
             'participant_created': False, 'cyclonedds_version': '0.10.5',
             'available': True, 'probe_exit_code': 0, 'tracing_config_supported': True,
             'installed_config_header': reference(config_header),
             'installed_version_header': reference(header),
             'loaded_provenance': {'pid': 23, 'process_start_ticks': '90',
                                   'process_identity_stable': True, 'vendor_loaded': True,
                                   'loaded_libraries': []}}
    args = dict(vendor='cyclonedds', version='0.10.5', observation_mode='instrumentation',
                carrier='ros', probe_reference=write_json(tmp_path / 'probe.json', probe),
                dependencies=[reference(header)])
    result = qualify_carrier(**args)
    assert result['status'] == 'unqualified'
    assert 'observer_adaptation_unverified' in result['issues']
    assert 'source_build_patch_identity_unavailable' in result['issues']
    assert result['identities']['source'] is None
    probe['loaded_provenance'] = []
    args['probe_reference'] = write_json(tmp_path / 'probe.json', probe)
    result = qualify_carrier(**args)
    assert result['status'] == 'unqualified'
    assert 'probe_process_identity_incomplete' in result['issues']


def test_qualification_does_not_execute_loader_or_native_probe(retained_carrier):
    from unittest.mock import patch
    args, _, _ = retained_carrier
    with patch('ctypes.CDLL', side_effect=AssertionError('native load forbidden')), \
            patch('subprocess.run', side_effect=AssertionError('process launch forbidden')):
        result = qualify_carrier(**args)
    assert result['status'] == 'artifact_qualified'
    assert result['run_eligible'] is False


def repin_build(args, build, probe):
    """Only synthetic fixture copies change; keep the independent probe binding."""
    args['build_reference'] = write_json(Path(args['build_reference']['path']), build)
    probe['build_manifest_sha256'] = args['build_reference']['sha256']
    args['probe_reference'] = write_json(Path(args['probe_reference']['path']), probe)


@pytest.mark.parametrize('process', [
    None, {},
    {'pid': 11, 'pgid': 12, 'sid': 10, 'start_ticks': 99, 'dedicated_process_group': True},
    {'pid': 11, 'pgid': 11, 'sid': 10, 'start_ticks': 99, 'dedicated_process_group': False},
    {'pid': 11, 'pgid': 11, 'sid': 10, 'dedicated_process_group': True},
    {'pid': True, 'pgid': True, 'sid': 10, 'start_ticks': 99, 'dedicated_process_group': True},
    {'pid': 11, 'pgid': 11, 'sid': 0, 'start_ticks': 99, 'dedicated_process_group': True},
    {'pid': 11, 'pgid': 11, 'sid': 10, 'start_ticks': -1, 'dedicated_process_group': True},
])
def test_build_requires_its_own_complete_dedicated_process_receipt(retained_carrier, process):
    args, build, probe = retained_carrier
    if process is None:
        del build['process']
    else:
        build['process'] = process
    repin_build(args, build, probe)
    result = qualify_carrier(**args)
    assert result['status'] == 'unqualified'
    assert 'build_process_identity_incomplete' in result['issues']
    assert result['identities']['build_process'] == process
    assert result['identities']['process']['pid'] == 17  # complete probe cannot fill build receipt
    assert result['run_eligible'] is False
    assert result['execution_evidence'] is False


@pytest.mark.parametrize('files', [
    {'normal.c': None}, {'normal.c': 'not-a-sha256'}, {'normal.c': 'A' * 64},
    {'normal.c': 123}, {'': '1' * 64}, {'/normal.c': '1' * 64},
    {'../normal.c': '1' * 64}, {'dir/../normal.c': '1' * 64},
])
def test_self_consistent_source_digest_does_not_qualify_invalid_inventory(retained_carrier, files):
    args, build, probe = retained_carrier
    source = {'files': files, 'sha256': hashlib.sha256(json.dumps(
        files, sort_keys=True, separators=(',', ':')).encode()).hexdigest()}
    build['source_frozen'] = source
    build['source_after'] = source
    repin_build(args, build, probe)
    result = qualify_carrier(**args)
    assert result['status'] == 'unqualified'
    assert 'source_build_identity_unverified' in result['issues']
    assert result['identities']['source'] == source
    assert result['run_eligible'] is False
    assert result['execution_evidence'] is False


@pytest.mark.parametrize('change', [
    'version', 'patch_id', 'source_root', 'archive_pin', 'source_state',
    'schema', 'file_set', 'unbound_file', 'source_conflict', 'invalid_digest',
    'original_digest', 'patched_digest', 'target_flag', 'provider',
    'patcher_digest', 'patcher_identity',
])
def test_fixed_observer_requires_patch_version_and_source_binding(retained_carrier, change):
    args, build, probe = retained_carrier
    patch = build['patch']
    name = 'include/fastdds/dds/publisher/DataWriter.hpp'
    if change == 'version':
        patch['vendor_version'] = '3.2.4'
    elif change == 'patch_id':
        patch['patch_id'] = 'unverified-other-release'
    elif change == 'source_root':
        patch['source_root'] += '-other'
    elif change == 'archive_pin':
        patch['reference_archive_sha256'] = '0' * 64
    elif change == 'source_state':
        patch['source_state'] = 'unpatched'
    elif change == 'schema':
        patch['schema_version'] = 2
    elif change == 'file_set':
        del patch['files'][name]
    elif change == 'unbound_file':
        patch['files']['not-in-source.hpp'] = patch['files'].pop(name)
    elif change == 'source_conflict':
        patch['files'][name]['sha256'] = '0' * 64
        patch['files'][name]['patched_sha256'] = '0' * 64
    elif change == 'invalid_digest':
        patch['files'][name]['sha256'] = None
    elif change == 'original_digest':
        patch['files'][name]['original_sha256'] = '1' * 64
    elif change == 'patched_digest':
        patch['files'][name]['patched_sha256'] = '1' * 64
    elif change == 'target_flag':
        patch['files'][name]['patch_target'] = False
    elif change == 'patcher_digest':
        patch['patcher_sha256'] = None
    elif change == 'patcher_identity':
        patch['patcher_sha256'] = '0' * 64
    else:
        patch['provider_sha256'] = '0' * 64
    repin_build(args, build, probe)
    result = qualify_carrier(**args)
    assert result['status'] == 'unqualified'
    assert 'patch_source_binding_unverified' in result['issues']
    assert result['identities']['patch'] == patch
    assert result['run_eligible'] is False
    assert result['execution_evidence'] is False


@pytest.mark.parametrize('change', [
    'wrong_archive_pin', 'missing_archive', 'relative_archive', 'other_archive_bytes',
    'missing_source_file', 'altered_source_file', 'extra_source_file', 'source_before',
])
def test_source_inventory_must_bind_the_retained_fixed_release_archive(retained_carrier, change, tmp_path):
    args, build, probe = retained_carrier
    name = 'README.md'  # outside the observer's header pins
    if change == 'wrong_archive_pin':
        build['archive_sha256'] = '0' * 64
    elif change == 'missing_archive':
        build['archive'] = str(tmp_path / 'missing-other-version.tar.gz')
    elif change == 'relative_archive':
        build['archive'] = 'source.tar.gz'
    elif change == 'other_archive_bytes':
        archive = tmp_path / 'unrelated.tar.gz'
        archive.write_bytes(b'unrelated archive, not fixed-release source')
        build['archive'] = str(archive)
        build['archive_sha256'] = reference(archive)['sha256']
    elif change == 'source_before':
        build['source_before']['files'][name] = '0' * 64
        build['source_before']['sha256'] = hashlib.sha256(json.dumps(
            build['source_before']['files'], sort_keys=True, separators=(',', ':')).encode()).hexdigest()
    else:
        files = build['source_frozen']['files']
        if change == 'missing_source_file':
            del files[name]
        elif change == 'altered_source_file':
            files[name] = '0' * 64
        else:
            files['extra.c'] = '1' * 64
        build['source_frozen']['sha256'] = hashlib.sha256(json.dumps(
            files, sort_keys=True, separators=(',', ':')).encode()).hexdigest()
        build['source_after'] = deepcopy(build['source_frozen'])
    repin_build(args, build, probe)
    result = qualify_carrier(**args)
    assert result['status'] == 'unqualified'
    assert 'source_archive_binding_unverified' in result['issues']
    assert result['identities']['source_archive'] == {
        'path': build['archive'], 'sha256': build['archive_sha256']}
    assert result['run_eligible'] is False
    assert result['execution_evidence'] is False


@pytest.mark.parametrize('failure', ['error', 'command', 'both', 'failed_summary'])
def test_explicit_compiler_failures_override_positive_summary_and_preserve_receipts(retained_carrier, failure):
    args, build, probe = retained_carrier
    if failure != 'error':
        build['commands'][-1]['returncode'] = 2
    if failure != 'command':
        build['error'] = 'build_step_failed:provider-build:2'
        build['error_type'] = 'RuntimeError'
    if failure == 'failed_summary':
        build['status'] = 'failed'
    repin_build(args, build, probe)
    result = qualify_carrier(**args)
    assert result['status'] == 'unqualified'
    if failure != 'command':
        assert 'build_failure_recorded' in result['issues']
    if failure != 'error':
        assert 'build_command_failed:provider-build:2' in result['issues']
    assert result['identities']['build_error'] == build.get('error')
    assert result['identities']['build_error_type'] == build.get('error_type')
    assert result['identities']['build_commands'] == build['commands']
    assert result['identities']['source'] == build['source_frozen']
    assert result['identities']['patch'] == build['patch']
    assert result['identities']['build_process'] == build['process']
    assert result['run_eligible'] is False
    assert result['execution_evidence'] is False


def test_failed_command_mapping_cannot_hide_behind_positive_build_summary(retained_carrier):
    """Final Spec C09: commands is the failed provider-build object, not a list."""
    args, build, probe = retained_carrier
    failed = deepcopy(build['commands'][-1])
    failed['returncode'] = 2
    build['commands'] = failed
    repin_build(args, build, probe)
    result = qualify_carrier(**args)
    assert result['status'] == 'unqualified'
    assert 'build_commands_invalid' in result['issues']
    assert result['identities']['build_commands'] == failed
    assert result['run_eligible'] is False
    assert result['execution_evidence'] is False


def test_failed_non_object_command_cannot_be_skipped(retained_carrier):
    """Final Spec C10: preserve the failed nested receipt and reject its shape."""
    args, build, probe = retained_carrier
    build['commands'][-1] = ['provider-build', {'returncode': 2}]
    repin_build(args, build, probe)
    before = deepcopy(args)
    result = qualify_carrier(**args)
    assert result['status'] == 'unqualified'
    assert 'build_command_invalid:3' in result['issues']
    assert result['identities']['build_commands'] == build['commands']
    assert args == before
    assert result['run_eligible'] is False
    assert result['execution_evidence'] is False


@pytest.mark.parametrize('field,value', [
    ('name', None), ('name', ''), ('name', '   '), ('name', 7),
    ('argv', None), ('argv', []), ('argv', 'compiler source.cpp'),
    ('argv', ['']), ('argv', ['compiler', 7]),
])
def test_success_code_without_verifiable_command_identity_is_unqualified(retained_carrier, field, value):
    args, build, probe = retained_carrier
    build['commands'][-1][field] = value
    repin_build(args, build, probe)
    result = qualify_carrier(**args)
    assert result['status'] == 'unqualified'
    assert 'build_command_invalid:3' in result['issues']
    assert result['identities']['build_commands'] == build['commands']
    assert result['run_eligible'] is False
    assert result['execution_evidence'] is False


@pytest.mark.parametrize('change,value,issue', [
    ('container', None, 'build_commands_invalid'),
    ('container', [], 'build_commands_invalid'),
    ('container', {}, 'build_commands_invalid'),
    ('container', 'provider-build', 'build_commands_invalid'),
    ('entry', None, 'build_command_invalid:3'),
    ('entry', 17, 'build_command_invalid:3'),
    ('entry', 'provider-build', 'build_command_invalid:3'),
    ('returncode', None, 'build_command_failed:provider-build:None'),
    ('returncode', False, 'build_command_failed:provider-build:False'),
    ('returncode', 0.0, 'build_command_failed:provider-build:0.0'),
    ('returncode', '0', 'build_command_failed:provider-build:0'),
    ('returncode', 2, 'build_command_failed:provider-build:2'),
    ('returncode', -9, 'build_command_failed:provider-build:-9'),
    ('missing', 'returncode', 'build_command_failed:provider-build:None'),
    ('missing', 'name', 'build_command_invalid:3'),
    ('missing', 'argv', 'build_command_invalid:3'),
])
def test_supplied_command_receipt_errors_fail_closed(retained_carrier, change, value, issue):
    args, build, probe = retained_carrier
    if change == 'container':
        build['commands'] = value
    elif change == 'entry':
        build['commands'][-1] = value
    elif change == 'missing':
        del build['commands'][-1][value]
    else:
        build['commands'][-1]['returncode'] = value
    repin_build(args, build, probe)
    before = deepcopy(args)
    result = qualify_carrier(**args)
    assert result['status'] == 'unqualified'
    assert issue in result['issues']
    assert result['identities']['build_commands'] == build['commands']
    assert args == before
    assert result['run_eligible'] is False
    assert result['execution_evidence'] is False


@pytest.mark.parametrize('failed', [False, True])
def test_legacy_manifest_may_omit_commands_without_fabricating_receipts(retained_carrier, failed):
    """Synthetic legacy shape: no command receipt requirement in build/v1."""
    args, build, probe = retained_carrier
    del build['commands']
    if failed:
        build['error'] = 'build_step_failed:provider-build:2'
        build['error_type'] = 'RuntimeError'
    repin_build(args, build, probe)
    result = qualify_carrier(**args)
    if failed:
        assert result['status'] == 'unqualified'
        assert 'build_failure_recorded' in result['issues']
        assert result['identities']['build_error'] == build['error']
    else:
        assert result['status'] == 'artifact_qualified'
        assert result['issues'] == []
    assert result['identities']['build_commands'] is None
    assert result['run_eligible'] is False
    assert result['execution_evidence'] is False


def test_command_extensions_and_empty_argument_are_preserved(retained_carrier):
    args, build, probe = retained_carrier
    build['commands'][-1]['argv'].append('')
    build['commands'][-1]['diagnostic'] = {'note': 'retained extension'}
    repin_build(args, build, probe)
    result = qualify_carrier(**args)
    assert result['status'] == 'artifact_qualified'
    assert result['issues'] == []
    assert result['identities']['build_commands'] == build['commands']
    assert result['run_eligible'] is False
    assert result['execution_evidence'] is False
