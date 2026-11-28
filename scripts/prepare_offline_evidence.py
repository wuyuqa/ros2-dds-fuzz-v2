#!/usr/bin/env python3
"""Install a pinned F1/P16 historical capsule without rewriting evidence records.

Default destination preserves the original absolute path bindings. --target is
for byte-level installer tests only; relocation does not qualify the evidence.
Existing destinations are verified read-only, never repaired or overwritten.
"""
import argparse
import hashlib
import io
import json
import os
from pathlib import Path, PurePosixPath
import tarfile

FIXTURES = Path(__file__).resolve().parents[1] / 'tests/fixtures'
CAPSULES = {
    'f1': {
        'stem': 'f1-5978d067',
        'root': Path('/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/'
                     's1-frontier-repair-20261118/f1-control-owner/isolated-scope-v2/live-output'),
        'archive_sha256': 'baae01dac89c59bfba69f8f952eaf70c5ed91f0167ecd8e0660f9cec02c8b140',
        'manifest_sha256': 'f5a6bfc4f2d51cd7ec3067ecc4d40295f0f0f92d5a3fffca08dd1f247b7010ec',
    },
    'p16': {
        'stem': 'p16-db4f12e',
        'root': Path('/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-final-breadth-prep-20261120/continuation-db4f12e/live-remaining-fifteen/partition-16'),
        'archive_sha256': '3aa367d886d81cd747cdcde08617dd8c68040e5adc5ecb8d0534e86d4eaf81f2',
        'manifest_sha256': '2ead2981f35df0f127bade992b05f345a139eae567670040e318eba1ffb3f7be',
    },
}


def digest(data):
    return hashlib.sha256(data).hexdigest()


def check_ancestors(path):
    # Do not resolve away symlinks before checking them.
    for part in (*reversed(path.parents), path):
        if part.is_symlink():
            raise ValueError(f'symlink in target path: {part}')


def validated_files(archive_path, capsule):
    manifest_bytes = (FIXTURES / (capsule['stem'] + '.manifest.json')).read_bytes()
    if digest(manifest_bytes) != capsule['manifest_sha256']:
        raise ValueError('pinned manifest hash mismatch')
    manifest = json.loads(manifest_bytes)
    data = archive_path.read_bytes()
    if digest(data) != capsule['archive_sha256'] or len(data) != manifest['archive_size']:
        raise ValueError('pinned archive hash/size mismatch')
    files = {}
    with tarfile.open(fileobj=io.BytesIO(data), mode='r:gz') as archive:
        for member in archive:
            name = member.name
            path = PurePosixPath(name)
            if (not member.isfile() or path.is_absolute() or '..' in path.parts
                    or path.as_posix() != name or '\\' in name or name in files):
                raise ValueError(f'unsafe archive member: {name}')
            expected = manifest['members'].get(name)
            if expected is None or member.size != expected['size']:
                raise ValueError(f'unexpected archive member/size: {name}')
            with archive.extractfile(member) as stream:
                content = stream.read()
            if digest(content) != expected['sha256']:
                raise ValueError(f'member hash mismatch: {name}')
            files[name] = content
    if files.keys() != manifest['members'].keys():
        raise ValueError('missing archive members')
    return files


def prepare(target, files):
    check_ancestors(target)
    if target.exists():
        if not target.is_dir():
            raise ValueError(f'existing target is not a directory: {target}')
        expected_dirs = {parent.as_posix() for name in files
                         for parent in PurePosixPath(name).parents if str(parent) != '.'}
        actual_files, actual_dirs = set(), set()
        for entry in target.rglob('*'):
            name = entry.relative_to(target).as_posix()
            if entry.is_symlink():
                raise ValueError(f'symlink in existing evidence: {entry}')
            if entry.is_dir():
                actual_dirs.add(name)
            elif entry.is_file():
                actual_files.add(name)
            else:
                raise ValueError(f'non-regular existing evidence: {entry}')
        if actual_files != files.keys() or actual_dirs != expected_dirs:
            raise ValueError('existing evidence member set mismatch; refusing overwrite')
        for name, content in files.items():
            if (target / name).read_bytes() != content:
                raise ValueError(f'existing evidence hash mismatch; refusing overwrite: {name}')
        return 'verified_existing'
    # The caller prepares the explicit parent; no privileged mkdir in this CLI.
    target.mkdir()
    for name, content in files.items():
        destination = target / name
        destination.parent.mkdir(parents=True, exist_ok=True)
        with destination.open('xb') as stream:
            stream.write(content)
    return 'installed'


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--capsule', choices=CAPSULES, default='f1')
    parser.add_argument('--target', type=Path)
    parser.add_argument('--archive', type=Path,
                        help='Archive to verify against the fixed pin (e.g. a downloaded copy)')
    args = parser.parse_args()
    capsule = CAPSULES[args.capsule]
    archive = args.archive or FIXTURES / (capsule['stem'] + '.tar.gz')
    try:
        target = Path(os.path.abspath(args.target or capsule['root']))
        files = validated_files(archive, capsule)
        status = prepare(target, files)
    except (OSError, ValueError, tarfile.TarError) as error:
        parser.exit(1, f'offline evidence: {error}\n')
    print(json.dumps({'status': status, 'target': str(target), 'files': len(files),
                      'capsule': args.capsule, 'archive_sha256': capsule['archive_sha256'],
                      'historical_path_binding': target == capsule['root'],
                      'new_execution': False}))


if __name__ == '__main__':
    main()
