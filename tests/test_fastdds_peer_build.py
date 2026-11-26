"""Offline build-contract checks; no compiler, ROS or DDS is started here."""
import hashlib
import io
import json
from pathlib import Path
import tarfile

import pytest

from r2f.instrumentation.fastdds_peer import build as b


def archive_tree(tmp_path, monkeypatch):
    source = tmp_path / 'source'
    source.mkdir()
    (source / 'file.txt').write_bytes(b'official-source')
    archive = tmp_path / 'source.tar.gz'
    with tarfile.open(archive, 'w:gz') as out:
        row = tarfile.TarInfo('Fast-DDS-2.14.7/file.txt')
        row.size = len(b'official-source')
        out.addfile(row, io.BytesIO(b'official-source'))
    monkeypatch.setattr(b, 'REFERENCE_ARCHIVE_SHA256', b.digest(archive))
    return archive, source


def test_whole_tree_matches_reference(tmp_path, monkeypatch):
    archive, source = archive_tree(tmp_path, monkeypatch)
    identity = b.verify_archive_tree(archive, source)
    assert identity['files'] == {'file.txt': hashlib.sha256(b'official-source').hexdigest()}
    assert identity == b.tree_identity(source)


def test_archive_identity_is_not_auto_accepted(tmp_path, monkeypatch):
    archive, source = archive_tree(tmp_path, monkeypatch)
    monkeypatch.setattr(b, 'REFERENCE_ARCHIVE_SHA256', '0' * 64)
    with pytest.raises(ValueError, match='reference_archive_hash_mismatch'):
        b.verify_archive_tree(archive, source)


@pytest.mark.parametrize('change', ['content', 'extra', 'missing', 'symlink'])
def test_tree_mismatch_rejected(tmp_path, monkeypatch, change):
    archive, source = archive_tree(tmp_path, monkeypatch)
    if change == 'content':
        (source / 'file.txt').write_bytes(b'altered')
    elif change == 'extra':
        (source / 'extra').write_bytes(b'injected')
    elif change == 'missing':
        (source / 'file.txt').unlink()
    else:
        (source / 'extra').symlink_to(source / 'file.txt')
    with pytest.raises((ValueError, OSError)):
        b.verify_archive_tree(archive, source)


def test_original_or_exact_patched_header_only(tmp_path, monkeypatch):
    archive, source = archive_tree(tmp_path, monkeypatch)
    monkeypatch.setattr(b, 'PATCH_ANCHORS', {'file.txt': 'header'})
    monkeypatch.setattr(b, 'patch_bytes', lambda path, data: data + b' friend')
    (source / 'file.txt').write_bytes(b'official-source friend')
    assert b.verify_archive_tree(archive, source) == b.tree_identity(source)
    (source / 'file.txt').write_bytes(b'official-source friend extra')
    with pytest.raises(ValueError, match='archive_source_mismatch'):
        b.verify_archive_tree(archive, source)


def test_build_environment_does_not_inherit_hooks_or_compiler_overrides(tmp_path):
    env = b.build_environment(tmp_path, {'PATH': '/usr/bin', 'LD_PRELOAD': '/bad',
        'LD_AUDIT': '/bad', 'CXXFLAGS': '-DABI_CHANGED', 'GCC_EXEC_PREFIX': '/bad',
        'COMPILER_PATH': '/bad', 'LD_LIBRARY_PATH': '/foreign', 'HOME': '/home/user'})
    assert not set(env) & {'LD_PRELOAD', 'LD_AUDIT', 'CXXFLAGS', 'GCC_EXEC_PREFIX', 'COMPILER_PATH'}
    assert env['PATH'] == str(tmp_path / 'usr/bin') + ':/usr/bin'
    assert env['LD_LIBRARY_PATH'] == str(tmp_path / 'usr/lib/x86_64-linux-gnu') + ':/opt/ros/jazzy/lib'
    assert env['HOME'] == '/home/user'


@pytest.mark.parametrize('arguments', [True, False])
def test_provider_reuses_vendor_compile_definitions_and_generated_headers(tmp_path, arguments):
    import shlex
    build, install = tmp_path / 'build', tmp_path / 'install'
    build.mkdir()
    source = str(tmp_path / 'source/fastdds/publisher/DataWriter.cpp')
    argv = ['/compiler', '-DFASTDDS_STATISTICS', '-DHAVE_SECURITY=1', '-I/source',
            '-isystem', '/include with spaces', '-std=c++14', '-fPIC', '-MD',
            '-MT', 'object', '-MF', 'object.d', '-o', 'object.o', '-c', source]
    row = {'file': source, 'directory': str(build)}
    row['arguments' if arguments else 'command'] = argv if arguments else shlex.join(argv)
    (build / 'compile_commands.json').write_text(json.dumps([row]))
    command, reference = b.provider_command('/source', build, install, '/cxx', '/provider.cpp', '/provider.so')
    assert command[0] == '/cxx'
    assert command[1:8] == argv[1:8]
    assert '-c' not in command and source not in command
    assert not set(command) & {'object', 'object.o', 'object.d', '-MT', '-MF', '-MD'}
    assert command[-2:] == ['-o', '/provider.so']
    assert '-Wl,-z,defs' in command and '-lfastrtps' in command
    assert reference == row


@pytest.mark.parametrize('count', [0, 2])
def test_ambiguous_vendor_reference_refused(tmp_path, count):
    row = {'file': '/fastdds/publisher/DataWriter.cpp', 'command': '/cxx -c source', 'directory': '/build'}
    (tmp_path / 'compile_commands.json').write_text(json.dumps([row] * count))
    with pytest.raises(ValueError, match='vendor_compile_command_not_unique'):
        b.provider_command('/source', tmp_path, '/install', '/cxx', '/provider.cpp', '/provider.so')


@pytest.mark.parametrize('jobs', [0, 33, True])
def test_invalid_parallelism_rejected_before_build(tmp_path, jobs):
    with pytest.raises(ValueError, match='jobs_must_be_1_to_32'):
        b.run_build(archive='/missing', source=tmp_path / 'source', root=tmp_path / 'root',
                    toolchain=tmp_path / 'toolchain', jobs=jobs)


def fake_build_setup(tmp_path, monkeypatch, fail=None):
    source, toolchain = tmp_path / 'source', tmp_path / 'toolchain'
    source.mkdir()
    bin_dir = toolchain / 'usr/bin'
    bin_dir.mkdir(parents=True)
    for name in ('x86_64-linux-gnu-gcc-13', 'x86_64-linux-gnu-g++-13', 'make'):
        (bin_dir / name).write_bytes(b'fake-tool')
    monkeypatch.setattr(b, 'verify_archive_tree', lambda *a: {'sha256': 'before', 'files': {}})
    monkeypatch.setattr(b, 'patch_vendor', lambda *a: {'abi': {}})
    monkeypatch.setattr(b, 'tree_identity', lambda *a: {'sha256': 'frozen', 'files': {}})
    commands = []

    def fake_run(command, **kwargs):
        from types import SimpleNamespace
        commands.append(command)
        step = ('configure' if '-S' in command else
                'install' if '--target' in command else
                'vendor' if '--build' in command else 'provider')
        if step == fail:
            return SimpleNamespace(returncode=7)
        root = tmp_path / 'root'
        if step == 'configure':
            build = root / 'build'
            build.mkdir()
            (build / 'CMakeCache.txt').write_text('fake cache')
            row = {'directory': str(build),
                   'file': str(source / 'fastdds/publisher/DataWriter.cpp'),
                   'arguments': ['/cxx', '-DFASTDDS_STATISTICS', '-c',
                                 str(source / 'fastdds/publisher/DataWriter.cpp')]}
            (build / 'compile_commands.json').write_text(json.dumps([row]))
        elif step == 'install':
            lib = root / 'install/lib'
            lib.mkdir(parents=True)
            (lib / 'libfastrtps.so').write_bytes(b'fake vendor')
        elif step == 'provider':
            Path(command[-1]).write_bytes(b'fake provider')
        return SimpleNamespace(returncode=0)

    monkeypatch.setattr(b.subprocess, 'run', fake_run)
    return source, toolchain, commands


def test_make_install_runs_preinstall_relink(tmp_path, monkeypatch):
    source, toolchain, commands = fake_build_setup(tmp_path, monkeypatch)
    path = b.run_build(archive=tmp_path / 'archive', source=source, root=tmp_path / 'root',
                       toolchain=toolchain, jobs=2)
    receipt = json.loads(path.read_text())
    assert receipt['status'] == 'built'
    assert receipt['verification'] == {'compilation': 'passed', 'dds': 'not_run', 'campaign': 'not_run'}
    assert commands[2] == ['cmake', '--build', str(tmp_path / 'root/build'),
                            '--target', 'install', '--parallel', '2']
    assert '--install' not in commands[2]
    assert receipt['binaries']['vendor']['sha256'] == hashlib.sha256(b'fake vendor').hexdigest()


@pytest.mark.parametrize('failed_step', ['vendor', 'install', 'provider'])
def test_failed_attempt_keeps_receipt_and_does_not_claim_compile_success(tmp_path, monkeypatch, failed_step):
    source, toolchain, commands = fake_build_setup(tmp_path, monkeypatch, fail=failed_step)
    with pytest.raises(RuntimeError, match='build_step_failed'):
        b.run_build(archive=tmp_path / 'archive', source=source, root=tmp_path / 'root',
                    toolchain=toolchain, jobs=2)
    receipts = list((tmp_path / 'root').glob('attempt-*/build-manifest.json'))
    assert len(receipts) == 1
    receipt = json.loads(receipts[0].read_text())
    assert receipt['status'] == 'failed'
    assert receipt['commands'][-1]['returncode'] == 7
    assert receipt['verification']['compilation'] == 'not_run'
    assert 'binaries' not in receipt


@pytest.mark.parametrize('missing', ['archive', 'compiler', 'provider'])
def test_prebuild_failure_is_preserved_without_starting_compiler(tmp_path, monkeypatch, missing):
    source, toolchain, commands = fake_build_setup(tmp_path, monkeypatch)
    if missing == 'archive':
        def fail_archive(*args):
            raise FileNotFoundError('missing-reference-archive')
        monkeypatch.setattr(b, 'verify_archive_tree', fail_archive)
    elif missing == 'compiler':
        (toolchain / 'usr/bin/x86_64-linux-gnu-gcc-13').unlink()
    else:
        monkeypatch.setattr(b, 'provider_source', lambda: tmp_path / 'missing-provider.cpp')
    with pytest.raises(FileNotFoundError):
        b.run_build(archive=tmp_path / 'archive', source=source, root=tmp_path / 'root',
                    toolchain=toolchain, jobs=2)
    assert commands == []
    path, = (tmp_path / 'root').glob('attempt-*/build-manifest.json')
    receipt = json.loads(path.read_text())
    assert receipt['status'] == 'failed'
    assert receipt['stage'] == {'archive': 'verify_archive', 'compiler': 'toolchain_identity',
                                 'provider': 'provider_snapshot'}[missing]
    assert receipt['error_type'] == 'FileNotFoundError'
    assert receipt['commands'] == []
    assert receipt['verification']['compilation'] == 'not_run'


def test_lock_conflict_does_not_make_an_attempt_or_touch_other_owner(tmp_path, monkeypatch):
    import fcntl
    source, toolchain, commands = fake_build_setup(tmp_path, monkeypatch)
    root = tmp_path / 'root'
    root.mkdir()
    with (root / '.build.lock').open('a') as owner:
        fcntl.flock(owner, fcntl.LOCK_EX | fcntl.LOCK_NB)
        with pytest.raises(BlockingIOError):
            b.run_build(archive=tmp_path / 'archive', source=source, root=root,
                        toolchain=toolchain, jobs=2)
    assert not list(root.glob('attempt-*')) and commands == []


def test_local_api_is_explicitly_compiled_and_receipted_without_changing_peer_source(tmp_path, monkeypatch):
    source, toolchain, commands = fake_build_setup(tmp_path, monkeypatch)
    peer_before = b.provider_source().read_bytes()
    path = b.run_build(archive=tmp_path / 'archive', source=source, root=tmp_path / 'root',
                       toolchain=toolchain, jobs=2, include_local_api=True)
    receipt = json.loads(path.read_text())
    extension = receipt['local_api']
    assert extension['schema'] == 'r2f.fastdds-local-api-build/v1'
    assert extension['source_sha256'] == hashlib.sha256(Path(extension['source_snapshot']).read_bytes()).hexdigest()
    assert extension['provider_sha256'] == receipt['binaries']['provider']['sha256']
    command = next(c for c in receipt['commands'] if c['name'] == 'provider-build')
    assert extension['source_snapshot'] in command['argv']
    assert str(path.parent / 'provider.cpp') in command['argv']
    assert b.provider_source().read_bytes() == peer_before
    assert receipt['provider_source_sha256'] == hashlib.sha256(peer_before).hexdigest()


def test_default_peer_build_does_not_grant_local_api(tmp_path, monkeypatch):
    source, toolchain, commands = fake_build_setup(tmp_path, monkeypatch)
    path = b.run_build(archive=tmp_path / 'archive', source=source, root=tmp_path / 'root',
                       toolchain=toolchain, jobs=2)
    assert 'local_api' not in json.loads(path.read_text())
    assert not any('local_api.cpp' in str(arg) for command in commands for arg in command)
