#!/usr/bin/env python3
"""Hash a separately installed Cyclone Python environment; never initialize DDS."""
import argparse
import hashlib
from importlib.metadata import distributions
import json
from pathlib import Path
import subprocess
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'src'))
from r2f.runtime.discovery_tools import load_tools_environment


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--python-root', type=Path, required=True)
    parser.add_argument('--library', type=Path, required=True)
    parser.add_argument('--source-repo', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    root, library, source = (p.resolve() for p in (args.python_root, args.library, args.source_repo))
    versions = {d.metadata['Name']: d.version for d in distributions(path=[str(root)])}
    if 'cyclonedds' not in versions:
        raise ValueError('cyclonedds_distribution_metadata_required')
    def git(*argv):
        return subprocess.check_output(['git', '-C', str(source), *argv], text=True).strip()
    paths = [p for p in root.rglob('*') if p.is_file() and '__pycache__' not in p.parts
             and (p.suffix in ('.py', '.so') or p.name in ('METADATA', 'entry_points.txt'))]
    files = {str(p): hashlib.sha256(p.read_bytes()).hexdigest() for p in sorted(set(paths + [library]))}
    value = {'schema': 'r2f.discovery-python-environment/v1', 'distribution': 'cyclonedds',
        'version': versions['cyclonedds'], 'python_root': str(root), 'files': files,
        'installed_distributions': versions,
        'library': {'path': str(library), 'sha256': files[str(library)]},
        'source': {'repo': str(source), 'commit': git('rev-parse', 'HEAD'),
            'status': git('status', '--porcelain'),
            'equivalence': 'installed_files_hashed_not_reproducible_build_proof'},
        'execution_evidence': False}
    data = json.dumps(value, indent=2, sort_keys=True).encode()+b'\n'
    ref = {'path': str(args.output.resolve()), 'sha256': hashlib.sha256(data).hexdigest()}
    with args.output.open('xb') as handle:
        handle.write(data)
    load_tools_environment(ref)
    print(json.dumps(ref, sort_keys=True))


if __name__ == '__main__':
    main()
