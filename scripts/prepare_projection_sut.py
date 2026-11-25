#!/usr/bin/env python3
"""Hash an independently built Cyclone SUT; never build, install or initialize DDS."""
import argparse
import hashlib
import json
from pathlib import Path
import sys
import xml.etree.ElementTree as ET

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'src'))
from r2f.runtime.environment import LOCK_FILE
from r2f.runtime.projection_sut import load_sut_environment, RMW


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ('rmw-prefix', 'core-prefix', 'rmw-source', 'core-source', 'build-record', 'output'):
        parser.add_argument('--' + name, type=Path, required=True)
    parser.add_argument('--rmw-commit', required=True)
    parser.add_argument('--core-commit', required=True)
    parser.add_argument('--core-version', required=True)
    args = parser.parse_args()
    rmw, core, rmw_source, core_source, record = (path.resolve() for path in
        (args.rmw_prefix, args.core_prefix, args.rmw_source, args.core_source, args.build_record))
    sha = lambda path: hashlib.sha256(path.read_bytes()).hexdigest()
    roots = (rmw, core, rmw_source, core_source)
    files = {str(path): sha(path) for root in roots for path in sorted(root.rglob('*'))
             if path.is_file() and '.git' not in path.relative_to(root).parts
             and '__pycache__' not in path.relative_to(root).parts}
    build = json.loads(record.read_text())
    for path, digest in build['files'].items():
        if sha(Path(path)) != digest:
            raise ValueError('projection_sut_build_record_changed:' + path)
        files[path] = digest
    files[str(record)] = sha(record)
    library = rmw / 'lib' / ('lib' + RMW + '.so')
    xml = rmw / 'share' / RMW / 'package.xml'
    core_library = (core / 'lib/libddsc.so').resolve(strict=True)
    manifest = {'schema': 'r2f.projection-sut-environment/v1',
        'base_dependency_lock_sha256': sha(LOCK_FILE),
        'rmw': {'prefix': str(rmw), 'version': ET.parse(xml).getroot().findtext('version'),
                'library': str(library), 'library_sha256': sha(library), 'package_xml_sha256': sha(xml)},
        'core': {'prefix': str(core), 'version': args.core_version,
                 'library': str(core_library), 'library_sha256': sha(core_library)},
        'source': [{'root': str(rmw_source), 'origin': 'ros2/rmw_cyclonedds',
                    'upstream_commit': args.rmw_commit},
                   {'root': str(core_source), 'origin': 'eclipse-cyclonedds/cyclonedds',
                    'upstream_commit': args.core_commit}],
        'build': {'kind': 'backport_sut', 'instrumented': False,
                  'record': {'path': str(record), 'sha256': sha(record)}},
        'files': files, 'execution_evidence': False,
        'source_binary_equivalence': 'source_build_binary_identities_separate_not_reproducible_build_proof'}
    data = json.dumps(manifest, indent=2, sort_keys=True).encode() + b'\n'
    ref = {'path': str(args.output.resolve()), 'sha256': hashlib.sha256(data).hexdigest()}
    with args.output.open('xb') as handle:
        handle.write(data)
    load_sut_environment(ref)
    print(json.dumps(ref, sort_keys=True))


if __name__ == '__main__':
    main()
