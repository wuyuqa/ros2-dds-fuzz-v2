"""Build an isolated, explicitly instrumented Fast DDS peer snapshot provider.

No package installation, ROS context or DDS entity is created. The caller owns
an unpacked official 2.14.7 source tree and a rootless GCC13 toolchain. Build
receipts preserve exact source/compiler/configuration/binary identities; they
are compile evidence, not proof that a DDS snapshot or a campaign passed.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import fcntl
import hashlib
import json
import os
from pathlib import Path
import subprocess
import tarfile
import uuid

from .patch_vendor import (PATCH_ANCHORS, REFERENCE_ARCHIVE_SHA256,
                           PatchError, patch_bytes, patch_vendor)

SCHEMA = 'r2f.fastdds-peer-build/v1'


def digest(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def tree_identity(root):
    root = Path(root)
    files = {}
    for path in sorted(root.rglob('*')):
        if path.is_symlink():
            raise ValueError('source_symlink:' + str(path.relative_to(root)))
        if path.is_file():
            files[str(path.relative_to(root))] = digest(path)
    encoded = json.dumps(files, sort_keys=True, separators=(',', ':')).encode()
    return {'sha256': hashlib.sha256(encoded).hexdigest(), 'files': files}


def verify_archive_tree(archive, source):
    """Check the WHOLE source tree, allowing only our exact six-header patch."""
    if digest(archive) != REFERENCE_ARCHIVE_SHA256:
        raise ValueError('reference_archive_hash_mismatch')
    expected = {}
    with tarfile.open(archive) as stream:
        for member in stream:
            parts = Path(member.name).parts
            if not parts or parts[0] != 'Fast-DDS-2.14.7' or '..' in parts:
                raise ValueError('unexpected_archive_path:' + member.name)
            if member.isdir():
                continue
            if not member.isfile():
                raise ValueError('non_regular_archive_member:' + member.name)
            relative = str(Path(*parts[1:]))
            if relative in expected:
                raise ValueError('duplicate_archive_member:' + relative)
            data = stream.extractfile(member).read()
            current = (Path(source) / relative).read_bytes()
            if relative in PATCH_ANCHORS:
                patched = patch_bytes(relative, data)
                if current not in (data, patched):
                    raise ValueError('archive_source_mismatch:' + relative)
            elif current != data:
                raise ValueError('archive_source_mismatch:' + relative)
            expected[relative] = True
    actual = tree_identity(source)
    if set(actual['files']) != set(expected):
        raise ValueError('archive_source_file_set_mismatch')
    return actual


def build_environment(toolchain, base=None):
    env = dict(os.environ if base is None else base)
    # Do not let unrelated hooks or compiler-selection variables silently alter
    # this independent build. Every actual command remains in its receipt.
    for key in ('LD_PRELOAD', 'LD_AUDIT', 'CC', 'CXX', 'CFLAGS', 'CXXFLAGS',
                'CPPFLAGS', 'LDFLAGS', 'CPATH', 'C_INCLUDE_PATH', 'CPLUS_INCLUDE_PATH',
                'LIBRARY_PATH', 'GCC_EXEC_PREFIX', 'COMPILER_PATH'):
        env.pop(key, None)
    root = Path(toolchain).resolve()
    env['PATH'] = str(root / 'usr/bin') + os.pathsep + env.get('PATH', '')
    env['LD_LIBRARY_PATH'] = os.pathsep.join((str(root / 'usr/lib/x86_64-linux-gnu'),
                                            '/opt/ros/jazzy/lib'))
    return env


def provider_command(source, build, install, compiler, provider, output):
    """Use the real vendor target's flags, not guessed compile-time ABI flags."""
    import shlex
    rows = json.loads((Path(build) / 'compile_commands.json').read_text())
    selected = [row for row in rows if row['file'].endswith('/fastdds/publisher/DataWriter.cpp')]
    if len(selected) != 1:
        raise ValueError('vendor_compile_command_not_unique')
    row = selected[0]
    argv = row.get('arguments') or shlex.split(row['command'])
    flags = []
    index = 1
    while index < len(argv):
        token = argv[index]
        if token in ('-o', '-MF', '-MT', '-MQ'):
            index += 2
            continue
        if token in ('-c', '-MD', '-MMD', '-MP') or token == row['file']:
            index += 1
            continue
        flags.append(token)
        index += 1
    # A standalone DSO has its own export list; keep vendor ABI definitions and
    # generated config/includes, but do not inherit its linker version script.
    command = [str(compiler), *flags, '-shared', '-fPIC', str(provider),
               '-L' + str(Path(install) / 'lib'), '-lfastrtps',
               '-Wl,-rpath,' + str(Path(install) / 'lib'),
               '-Wl,-z,defs', '-o', str(output)]
    return command, row


def provider_source():
    return Path(__file__).with_name('provider.cpp')


def run_build(*, archive, source, root, toolchain, jobs=8):
    source, root, toolchain = (Path(p).resolve() for p in (source, root, toolchain))
    if type(jobs) is not int or not 1 <= jobs <= 32:
        raise ValueError('jobs_must_be_1_to_32')
    if root == source or root.is_relative_to(source):
        raise ValueError('build_root_must_be_outside_source')
    root.mkdir(parents=True, exist_ok=True)
    with (root / '.build.lock').open('a') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        build, install = root / 'build', root / 'install'
        attempt = root / ('attempt-' + datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S') + '-' + uuid.uuid4().hex[:8])
        attempt.mkdir()
        commands = []
        # Establish the failure sink BEFORE reading or mutating any input.
        # A lock conflict above deliberately does not write another owner's run.
        receipt = {'schema': SCHEMA, 'status': 'preparing', 'stage': 'prepare',
                   'observation_mode': 'instrumentation', 'vendor_version': '2.14.7',
                   'archive': str(Path(archive).resolve()), 'source': str(source),
                   'build': str(build), 'install': str(install), 'attempt': str(attempt),
                   'commands': commands,
                   'verification': {'compilation': 'not_run', 'dds': 'not_run', 'campaign': 'not_run'}}
        env = build_environment(toolchain)

        def execute(name, command, *, cwd=None):
            receipt['stage'] = name
            log = attempt / (name + '.log')
            row = {'name': name, 'argv': list(map(str, command)), 'cwd': str(cwd or root), 'log': str(log)}
            commands.append(row)
            print(name + ': ' + str(log), flush=True)
            with log.open('wb') as stream:
                completed = subprocess.run(row['argv'], cwd=cwd or root, env=env,
                                           stdout=stream, stderr=subprocess.STDOUT)
            row['returncode'] = completed.returncode
            if completed.returncode:
                raise RuntimeError('build_step_failed:' + name + ':' + str(completed.returncode))

        try:
            receipt['stage'] = 'process_identity'
            start_ticks = int(Path('/proc/self/stat').read_text().rsplit(')', 1)[1].split()[19])
            receipt['process'] = {'pid': os.getpid(), 'pgid': os.getpgrp(),
                                  'sid': os.getsid(0), 'start_ticks': start_ticks,
                                  'dedicated_process_group': os.getpid() == os.getpgrp()}
            receipt['stage'] = 'verify_archive'
            receipt['source_before'] = verify_archive_tree(archive, source)
            receipt['archive_sha256'] = REFERENCE_ARCHIVE_SHA256
            receipt['stage'] = 'patch'
            receipt['patch'] = patch_vendor(source)
            receipt['stage'] = 'source_identity'
            frozen = tree_identity(source)
            receipt['source_frozen'] = frozen
            receipt['stage'] = 'provider_snapshot'
            provider = provider_source()
            frozen_provider = attempt / 'provider.cpp'
            frozen_provider.write_bytes(provider.read_bytes())
            receipt['provider_source_sha256'] = digest(frozen_provider)
            receipt['builder_sha256'] = digest(__file__)
            receipt['stage'] = 'toolchain_identity'
            c = toolchain / 'usr/bin/x86_64-linux-gnu-gcc-13'
            cxx = toolchain / 'usr/bin/x86_64-linux-gnu-g++-13'
            make = toolchain / 'usr/bin/make'
            receipt['toolchain'] = {str(p): digest(p) for p in (c, cxx, make)}
            receipt['environment'] = {k: env[k] for k in ('PATH', 'LD_LIBRARY_PATH')}
            receipt['status'] = 'building'
            execute('configure', ['cmake', '-S', source, '-B', build, '-G', 'Unix Makefiles',
                    '-DCMAKE_MAKE_PROGRAM=' + str(make), '-DCMAKE_C_COMPILER=' + str(c),
                    '-DCMAKE_CXX_COMPILER=' + str(cxx), '-DCMAKE_BUILD_TYPE=Release',
                    '-DCMAKE_EXPORT_COMPILE_COMMANDS=ON', '-DCMAKE_INSTALL_PREFIX=' + str(install),
                    '-DCMAKE_PREFIX_PATH=/opt/ros/jazzy;/opt/ros/jazzy/lib/foonathan_memory',
                    '-DAsio_INCLUDE_DIR=' + str(toolchain / 'usr/include'),
                    '-DSECURITY=ON', '-DFASTDDS_STATISTICS=ON', '-DBUILD_TESTING=OFF',
                    '-DCOMPILE_EXAMPLES=OFF', '-DCOMPILE_TOOLS=OFF',
                    '-DSQLITE3_SUPPORT=ON', '-DTHIRDPARTY=OFF'])
            execute('vendor-build', ['cmake', '--build', build, '--parallel', str(jobs)])
            # Makefile generators may need the preinstall relink target before
            # copying ELF files; cmake --install alone does not run that target.
            execute('vendor-install', ['cmake', '--build', build, '--target', 'install',
                                       '--parallel', str(jobs)])
            output = install / 'lib/libr2f_fastdds_peer.so'
            command, reference = provider_command(source, build, install, cxx, frozen_provider, output)
            receipt['vendor_reference_command'] = reference
            execute('provider-build', command, cwd=reference['directory'])
            receipt['stage'] = 'final_identity'
            after = tree_identity(source)
            if after != frozen or digest(provider) != digest(frozen_provider):
                raise RuntimeError('source_changed_during_build')
            vendor = (install / 'lib/libfastrtps.so').resolve(strict=True)
            receipt.update(status='built', stage='complete', source_after=after,
                           binaries={'vendor': {'path': str(vendor), 'sha256': digest(vendor)},
                                     'provider': {'path': str(output), 'sha256': digest(output)}})
            receipt['compile_commands_sha256'] = digest(build / 'compile_commands.json')
            receipt['cmake_cache_sha256'] = digest(build / 'CMakeCache.txt')
            receipt['verification']['compilation'] = 'passed'
        except Exception as exc:
            receipt.update(status='failed', error=str(exc), error_type=type(exc).__name__)
            raise
        finally:
            (attempt / 'build-manifest.json').write_text(json.dumps(receipt, indent=2, sort_keys=True) + '\n')
        return attempt / 'build-manifest.json'


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--archive', required=True, type=Path)
    parser.add_argument('--source-root', required=True, type=Path)
    parser.add_argument('--build-root', required=True, type=Path)
    parser.add_argument('--toolchain-root', required=True, type=Path)
    parser.add_argument('--jobs', type=int, default=8)
    args = parser.parse_args(argv)
    try:
        manifest = run_build(archive=args.archive, source=args.source_root,
                             root=args.build_root, toolchain=args.toolchain_root, jobs=args.jobs)
        print(str(manifest))
        return 0
    except (ValueError, OSError, RuntimeError, PatchError) as exc:
        print('failed: ' + str(exc), flush=True)
        return 2


if __name__ == '__main__':
    raise SystemExit(main())
