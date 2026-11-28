"""Process-local evidence for the discovery profile presented at ROS init.

This records selected environment values and bytes readable immediately before
initialization. It deliberately does not infer vendor-effective settings or
semantic discovery behavior from those observations.
"""
from __future__ import annotations

import base64
import hashlib
import os
import stat
from collections.abc import Mapping
from pathlib import Path
import time
import xml.etree.ElementTree as ET

_SELECTORS = {
    'rmw_cyclonedds_cpp': ('CYCLONEDDS_URI', 'cyclonedds'),
    'rmw_fastrtps_cpp': ('FASTRTPS_DEFAULT_PROFILES_FILE', 'fastdds'),
}
_PACKAGES = {
    'rmw_cyclonedds_cpp': 'rmw_cyclonedds_cpp',
    'rmw_fastrtps_cpp': 'rmw_fastrtps_cpp',
}


def _read_profile(reference, vendor):
    row = {'reference': reference, 'vendor': vendor}
    if not isinstance(reference, str) or not reference or '\x00' in reference:
        return {**row, 'status': 'invalid_reference'}
    if reference.lstrip().startswith('<') or ';' in reference or ',' in reference:
        return {**row, 'status': 'unsupported_reference'}
    if reference.startswith('file://'):
        from urllib.parse import urlparse, unquote
        try:
            parsed = urlparse(reference)
        except ValueError:
            return {**row, 'status': 'unsupported_reference'}
        if parsed.scheme != 'file' or parsed.netloc not in ('', 'localhost') or parsed.query or parsed.fragment:
            return {**row, 'status': 'unsupported_reference'}
        path = Path(unquote(parsed.path))
    elif ':' in reference:
        return {**row, 'status': 'unsupported_reference'}
    else:
        path = Path(reference)
    try:
        fd = os.open(path, os.O_RDONLY | os.O_NONBLOCK)
        with os.fdopen(fd, 'rb') as handle:
            if not stat.S_ISREG(os.fstat(handle.fileno()).st_mode):
                return {**row, 'status': 'unsupported_reference'}
            data = handle.read(1024 * 1024 + 1)
        if len(data) > 1024 * 1024:
            return {**row, 'status': 'unsupported_size'}
    except (OSError, ValueError) as exc:
        return {**row, 'status': 'unreadable', 'error': type(exc).__name__}
    try:
        root = ET.fromstring(data)
    except (ET.ParseError, LookupError, ValueError):
        return {**row, 'status': 'invalid_xml', 'sha256': hashlib.sha256(data).hexdigest(),
                'bytes_base64': base64.b64encode(data).decode('ascii')}
    root_name = root.tag.rsplit('}', 1)[-1]
    expected = 'CycloneDDS' if vendor == 'cyclonedds' else 'dds'
    status = 'bytes_observed' if root_name == expected else 'vendor_root_mismatch'
    return {**row, 'status': status, 'path': str(path.resolve()),
            'root_element': root_name, 'sha256': hashlib.sha256(data).hexdigest(),
            'bytes_base64': base64.b64encode(data).decode('ascii')}


def _package_identity(name, package_prefix):
    row = {'name': name, 'basis': 'installed_package_manifest_not_loaded_library'}
    try:
        prefix = Path(package_prefix(name))
        manifest = prefix / 'share' / name / 'package.xml'
        fd = os.open(manifest, os.O_RDONLY | os.O_NONBLOCK)
        with os.fdopen(fd, 'rb') as handle:
            if not stat.S_ISREG(os.fstat(handle.fileno()).st_mode):
                return {**row, 'status': 'unsupported_file'}
            data = handle.read(1024 * 1024 + 1)
        if len(data) > 1024 * 1024:
            return {**row, 'status': 'unsupported_size'}
    except (LookupError, OSError, ValueError) as exc:
        return {**row, 'status': 'unavailable', 'error': type(exc).__name__}
    try:
        root = ET.fromstring(data)
    except (ET.ParseError, LookupError, ValueError) as exc:
        return {**row, 'status': 'invalid_manifest', 'error': type(exc).__name__,
                'package_xml_sha256': hashlib.sha256(data).hexdigest()}
    if root.tag != 'package' or root.findtext('name') != name:
        return {**row, 'status': 'invalid_manifest',
                'package_xml_sha256': hashlib.sha256(data).hexdigest()}
    version = root.findtext('version')
    if not isinstance(version, str) or not version.strip():
        return {**row, 'status': 'version_missing',
                'package_xml_sha256': hashlib.sha256(data).hexdigest()}
    return {**row, 'status': 'observed', 'version': version.strip(),
            'package_xml_sha256': hashlib.sha256(data).hexdigest()}


def initialize_with_profile_observation(*, environment, requested_rmw, initialize,
                                        rmw_identifier, package_prefix):
    """Snapshot allowlisted process inputs, run init, and return the RMW id/evidence."""
    if not isinstance(environment, Mapping) or any(not isinstance(k, str) or not isinstance(v, str)
                                                for k, v in environment.items()):
        raise TypeError('environment_must_be_string_mapping')
    if requested_rmw is not None and not isinstance(requested_rmw, str):
        raise TypeError('requested_rmw_must_be_string_or_none')
    if not callable(initialize) or not callable(rmw_identifier) or not callable(package_prefix):
        raise TypeError('observation_callbacks_must_be_callable')

    environment = dict(environment)
    sampled_ns = time.monotonic_ns()
    supplied_rmw = environment.get('RMW_IMPLEMENTATION')
    selector = _SELECTORS.get(supplied_rmw)
    selector_name = selector[0] if selector else None
    reference = environment.get(selector_name) if selector_name else None
    profiles = []
    if selector_name and reference is not None:
        profiles.append(_read_profile(reference, selector[1]))
    elif selector_name:
        profiles.append({'vendor': selector[1], 'status': 'selector_missing'})
    else:
        profiles.append({'status': 'unsupported_or_mixed_vendor',
                         'supplied_rmw': supplied_rmw})

    conflicting = [key for key in ('CYCLONEDDS_URI', 'FASTRTPS_DEFAULT_PROFILES_FILE',
                                   'FASTDDS_DEFAULT_PROFILES_FILE')
                   if key in environment and key != selector_name]
    profile_status = 'mixed_or_unsupported_selectors' if conflicting else profiles[0]['status']
    package_name = _PACKAGES.get(supplied_rmw)
    packages = [_package_identity(package_name, package_prefix)] if package_name else []
    begin = time.monotonic_ns()
    initialize()
    returned = time.monotonic_ns()
    observed_rmw = rmw_identifier()
    observed_status = 'observed' if isinstance(observed_rmw, str) and observed_rmw else 'invalid'
    receipt = {
        'schema_version': 1,
        'observation_basis': 'role_process_pre_init_file_read_not_vendor_readback',
        'sampled_before_init_monotonic_ns': sampled_ns,
        'loaded_library_identity': 'unverified',
        'requested_configuration': {'rmw_implementation': requested_rmw},
        'process_environment': {key: environment[key] for key in
                                ('RMW_IMPLEMENTATION', 'CYCLONEDDS_URI',
                                 'FASTRTPS_DEFAULT_PROFILES_FILE', 'FASTDDS_DEFAULT_PROFILES_FILE')
                                if key in environment},
        'configuration_status': ('consistent' if requested_rmw in _SELECTORS and requested_rmw == supplied_rmw
                                 else 'unknown_or_mismatched'),
        'profile_status': profile_status, 'profiles': profiles,
        'packages': packages,
        'initialization': {'status': 'returned', 'begin_monotonic_ns': begin,
                           'returned_monotonic_ns': returned, 'error': None},
        'runtime_rmw': {'status': observed_status, 'identifier': observed_rmw},
        'vendor_effective_settings': 'unknown', 'semantic_effect': 'unknown',
    }
    if observed_rmw and observed_rmw != supplied_rmw:
        receipt['runtime_rmw']['status'] = 'mixed_or_mismatched'
    return observed_rmw, receipt
