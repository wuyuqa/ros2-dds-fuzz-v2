"""Read-only qualification for the opt-in public local-API companion.

A legacy peer build remains a legacy peer build. Only a separately receipted
translation unit linked into the selected provider grants the local API.
"""
from copy import deepcopy
import hashlib
from pathlib import Path

SCHEMA = 'r2f.fastdds-local-api-build/v1'


def source_path():
    return Path(__file__).with_suffix('.cpp')


def digest(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def build_receipt(snapshot, provider):
    return {'schema': SCHEMA, 'source_snapshot': str(Path(snapshot).resolve()),
            'source_sha256': digest(snapshot), 'provider_sha256': digest(provider)}


def validate_extension(manifest):
    """Validate artifacts only; never load a DSO, compile, or create DDS."""
    value = manifest.get('local_api')
    fields = {'schema', 'source_snapshot', 'source_sha256', 'provider_sha256'}
    if not isinstance(value, dict) or set(value) != fields or value.get('schema') != SCHEMA:
        raise ValueError('local_api_build_receipt_missing_or_invalid')
    snapshot = value.get('source_snapshot')
    if (not isinstance(snapshot, str) or not Path(snapshot).is_absolute()
            or not isinstance(manifest.get('attempt'), str)
            or not Path(snapshot).resolve().is_relative_to(Path(manifest['attempt']).resolve())
            or value.get('source_sha256') != digest(source_path())
            or digest(snapshot) != value['source_sha256']):
        raise ValueError('local_api_source_identity_mismatch')
    provider = manifest.get('binaries', {}).get('provider', {})
    if (value.get('provider_sha256') != provider.get('sha256')
            or not isinstance(provider.get('path'), str)
            or digest(provider['path']) != value['provider_sha256']):
        raise ValueError('local_api_provider_identity_mismatch')
    commands = manifest.get('commands', [])
    matching = [c for c in commands if isinstance(c, dict)
                and isinstance(c.get('argv'), list) and snapshot in c['argv']]
    if (len(matching) != 1 or type(matching[0].get('returncode')) is not int
            or matching[0]['returncode'] != 0 or '-shared' not in matching[0]['argv']
            or '-fPIC' not in matching[0]['argv']
            or matching[0]['argv'][-2:] != ['-o', provider['path']]):
        raise ValueError('local_api_compilation_receipt_invalid')
    return deepcopy(value)
