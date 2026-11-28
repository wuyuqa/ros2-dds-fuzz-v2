"""Explicit, hash-bound Cyclone SUT builds; never an observation or DDS launcher."""
from pathlib import Path
import re

KEY = 'projection_sut_environment'
RMW = 'rmw_cyclonedds_cpp'
TARGETS = ('builtin.discovery_projection_reconciliation',
           'builtin.discovery_projection_reconciliation.fuzz_v1')


def validate_sut_reference(config):
    if KEY not in config:
        return
    if config.get('projection_discovery_layers') != 'participant_topic_v1':
        raise ValueError('projection_sut_requires_projection_discovery_layers')
    if (config.get('target_id', TARGETS[0]) not in TARGETS
            or config.get('rmw', RMW) != RMW
            or any(rmw != RMW for rmw in config.get('rmws', {}).values())):
        raise ValueError('projection_sut_requires_cyclone_projection')
    ref = config[KEY]
    if (not isinstance(ref, dict) or set(ref) != {'path', 'sha256'}
            or not isinstance(ref['path'], str) or not Path(ref['path']).is_absolute()
            or not isinstance(ref['sha256'], str) or not re.fullmatch('[0-9a-f]{64}', ref['sha256'])):
        raise ValueError('projection_sut_reference_invalid')


def load_sut_environment(ref):
    """Verify all build/source bytes, preserving the default underlay lock."""
    import hashlib
    import json
    from .environment import LOCK_FILE

    data = Path(ref['path']).read_bytes()
    if hashlib.sha256(data).hexdigest() != ref['sha256']:
        raise ValueError('projection_sut_manifest_hash_mismatch')
    manifest = json.loads(data)
    try:
        rmw, core, files = manifest['rmw'], manifest['core'], manifest['files']
        roots = [Path(rmw['prefix']), Path(core['prefix'])]
        roots.extend(Path(source['root']) for source in manifest['source'])
        required = [Path(rmw['prefix']) / 'lib' / ('lib' + RMW + '.so'),
                    Path(rmw['prefix']) / 'share' / RMW / 'package.xml',
                    Path(rmw['prefix']) / 'share/ament_index/resource_index/packages' / RMW,
                    Path(core['prefix']) / 'include/dds/features.h', Path(core['library']),
                    Path(manifest['build']['record']['path'])]
        if (manifest['schema'] != 'r2f.projection-sut-environment/v1'
                or manifest['base_dependency_lock_sha256'] != hashlib.sha256(LOCK_FILE.read_bytes()).hexdigest()
                or not manifest['source'] or not isinstance(files, dict) or not files
                or manifest['build']['kind'] != 'backport_sut'
                or manifest['build']['instrumented'] is not False
                or rmw['library'] != str(required[0])
                or not Path(core['library']).is_relative_to(Path(core['prefix']) / 'lib')
                or not all(root.is_absolute() and root.is_dir() for root in roots)
                or not all(str(path) in files for path in required)
                or not all(source.get('origin') and re.fullmatch('[0-9a-f]{40}', source['upstream_commit'])
                           for source in manifest['source'])
                or not all(isinstance(record['version'], str) and record['version']
                           for record in (rmw, core))
                or any(files[record['library']] != record['library_sha256'] for record in (rmw, core))
                or files[str(required[1])] != rmw['package_xml_sha256']
                or files[str(required[-1])] != manifest['build']['record']['sha256']):
            raise ValueError('projection_sut_manifest_incomplete')
    except (KeyError, TypeError, AttributeError) as exc:
        raise ValueError('projection_sut_manifest_incomplete') from exc
    for path, expected in files.items():
        if (not isinstance(path, str) or not Path(path).is_absolute() or not isinstance(expected, str)
                or not re.fullmatch('[0-9a-f]{64}', expected)
                or hashlib.sha256(Path(path).read_bytes()).hexdigest() != expected):
            raise ValueError('projection_sut_dependency_changed:' + path)
    for root in roots:
        actual = {str(path) for path in root.rglob('*') if path.is_file()
                  and '.git' not in path.relative_to(root).parts
                  and '__pycache__' not in path.relative_to(root).parts}
        expected = {path for path in files if Path(path).is_relative_to(root)}
        if not actual or actual != expected:
            raise ValueError('projection_sut_inventory_changed:' + str(root))
    return manifest


def verify_sut_libraries(sut, libraries):
    """Match an owned process's independently captured DDS/RMW mappings."""
    expected = {str(Path(sut[key]['library']).resolve()): sut[key]['library_sha256']
                for key in ('rmw', 'core')}
    selected = [row for row in libraries if Path(row['path']).name.startswith('libddsc.so')
                or Path(row['path']).name == 'lib' + RMW + '.so']
    actual = {str(Path(row['path']).resolve()): row['sha256'] for row in selected}
    if len(selected) != 2 or actual != expected:
        raise ValueError('projection_sut_worker_library_mismatch')
