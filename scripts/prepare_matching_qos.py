#!/usr/bin/env python3
"""Build a read/set public-API adapter, without importing ROS or running DDS."""
import argparse
import json
import re
from pathlib import Path
import shutil
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'src'))
from r2f.runtime.matching_qos import BUILD_SCHEMA, SOURCE, digest


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--vendor', choices=('fastdds', 'cyclonedds'), default='fastdds')
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--compiler', default=shutil.which('g++'))
    args = parser.parse_args()
    output = args.output.resolve()
    if not args.compiler:
        parser.error('C++ compiler unavailable; supply --compiler')
    compiler = Path(args.compiler).resolve(strict=True)
    if args.vendor == 'cyclonedds':
        from r2f.runtime.native_lookup_support import build_support
        value = build_support(output, compiler=compiler)
        print(json.dumps({'path': value['manifest_path'], 'sha256': value['manifest_sha256']}))
        return 0
    output.mkdir(parents=True, exist_ok=False)
    prefix = Path('/opt/ros/jazzy')
    library = output / 'libmatching_qos.so'
    deps = output / 'dependencies.d'
    argv = [str(compiler), '-std=c++17', '-O2', '-fPIC', '-shared', '-Wl,-z,defs',
            '-MMD', '-MF', str(deps), str(SOURCE), '-o', str(library)]
    argv += ['-I' + str(p) for p in sorted((prefix / 'include').iterdir()) if p.is_dir()]
    argv += ['-L' + str(prefix / 'lib'), '-Wl,-rpath,' + str(prefix / 'lib'),
             '-lrcl', '-lrmw_fastrtps_cpp', '-lfastrtps', '-lfastcdr']
    result = subprocess.run(argv, text=True, capture_output=True)
    (output / 'compiler.stdout').write_text(result.stdout)
    (output / 'compiler.stderr').write_text(result.stderr)
    if result.returncode:
        print(result.stderr, file=sys.stderr)
        return result.returncode
    files = {Path(p).resolve() for p in deps.read_text().replace('\\\n', '').split(':', 1)[1].split()}
    files.update([compiler, library])
    files.update((prefix / 'lib' / name).resolve(strict=True) for name in (
        'librcl.so', 'librmw_fastrtps_cpp.so', 'librmw_fastrtps_shared_cpp.so', 'libfastrtps.so', 'libfastcdr.so'))
    feature_headers = [prefix / 'include/fastrtps/fastdds/dds/domain/DomainParticipantListener.hpp',
                       prefix / 'include/fastrtps/fastdds/rtps/participant/RTPSParticipantListener.h']
    version_header = prefix / 'include/fastrtps/fastrtps/config.h'
    files.update(feature_headers + [version_header])
    version = re.search(r'#define FASTRTPS_VERSION_STR "([^"]+)"', version_header.read_text()).group(1)
    feature = ('available_unvalidated' if all('should_endpoints_match' in p.read_text() for p in feature_headers)
               else 'unavailable_in_installed_headers')
    manifest = {'schema': BUILD_SCHEMA, 'fastdds_version': version,
                'feature_availability': {'should_endpoints_match': feature}, 'source_sha256': digest(SOURCE),
                'sut_modified': False, 'library': str(library), 'compiler_argv': argv,
                'compiler_version': subprocess.check_output([str(compiler), '--version'], text=True),
                'files': {str(path): digest(path) for path in sorted(files)}}
    path = output / 'build.json'
    path.write_text(json.dumps(manifest, indent=2) + '\n')
    print(json.dumps({'path': str(path), 'sha256': digest(path)}))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
