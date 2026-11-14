"""Read-only ROS environment discovery; no rclpy context is created by probes."""
from __future__ import annotations
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys

from .domains import scan_old_locks, scan_processes, probe_rtps_ports

RMW_NAMES = ('rmw_cyclonedds_cpp', 'rmw_fastrtps_cpp')
REPO = Path(__file__).resolve().parents[3]
LOCK_FILE = REPO / 'config/dependencies.lock.json'
ENV_KEYS = ('PATH', 'PYTHONPATH', 'LD_LIBRARY_PATH', 'AMENT_PREFIX_PATH', 'CMAKE_PREFIX_PATH',
            'COLCON_PREFIX_PATH', 'ROS_DISTRO', 'ROS_VERSION', 'ROS_PYTHON_VERSION',
            'R2F_OVERLAY_KIND', 'R2F_OVERLAY_SOURCE')
PROBE_CODE = r'''
import ctypes, hashlib, importlib.util, json, os, pathlib, sys, xml.etree.ElementTree as ET
from ament_index_python.packages import get_package_prefix
result = {'python': sys.version, 'python_executable': sys.executable,
          'ros_distro': os.environ.get('ROS_DISTRO'), 'packages': {}, 'rmw': {},
          'activation_environment': {k: os.environ[k] for k in KEYS if k in os.environ}}
for package in ('rclpy', 'std_msgs', 'rmw_cyclonedds_cpp', 'rmw_fastrtps_cpp'):
    try:
        prefix = pathlib.Path(get_package_prefix(package))
        xml = prefix / 'share' / package / 'package.xml'
        record = {'prefix': str(prefix), 'version': ET.parse(xml).getroot().findtext('version'),
                  'package_xml_sha256': hashlib.sha256(xml.read_bytes()).hexdigest()}
        if package.startswith('rmw_'):
            lib = prefix / 'lib' / ('lib' + package + '.so')
            ctypes.CDLL(str(lib))
            record.update(library=str(lib), library_sha256=hashlib.sha256(lib.read_bytes()).hexdigest(),
                          available=True, availability_basis='shared_library_load_without_context')
            result['rmw'][package] = record
        result['packages'][package] = record
    except Exception as e:
        record = {'available': False, 'error': type(e).__name__ + ':' + str(e)}
        result['packages'][package] = record
        if package.startswith('rmw_'): result['rmw'][package] = record
for module in ('rclpy', 'std_msgs.msg'):
    __import__(module)
print(json.dumps(result, sort_keys=True))
'''


def source_identity(repo=REPO):
    repo = Path(repo).resolve()
    def git(*args):
        p = subprocess.run(['git', '-C', str(repo), *args], text=True, capture_output=True, timeout=10)
        return p.stdout.strip() if p.returncode == 0 else None
    files = sorted((repo / 'src/r2f/runtime').glob('*.py'))
    files += [repo / 'config/dependencies.lock.json', repo / 'scripts/setup_overlay.bash']
    digest = hashlib.sha256()
    for path in files:
        if path.exists():
            digest.update(str(path.relative_to(repo)).encode() + b'\0' + path.read_bytes())
    return {'source_repo': str(repo), 'head': git('rev-parse', 'HEAD'),
            'worktree_status': git('status', '--porcelain'), 'runtime_source_sha256': digest.hexdigest()}


def activated_environment(config):
    setup = Path(config.get('ros_setup', '/opt/ros/jazzy/setup.bash'))
    overlay = str(config.get('overlay_setup') or (REPO / 'scripts/setup_overlay.bash'))
    if not setup.is_file() or (overlay and not Path(overlay).is_file()):
        raise RuntimeError('blocked:ros_setup_missing')
    code = 'KEYS=' + repr(ENV_KEYS) + '\n' + PROBE_CODE
    command = ['bash', '--noprofile', '--norc', '-c',
               'source "$1" >/dev/null && { test -z "$2" || source "$2" >/dev/null; } && exec /usr/bin/python3 -c "$3"',
               'r2f-environment', str(setup), overlay, code]
    env = {k: v for k, v in os.environ.items() if k in ('HOME', 'USER', 'LANG', 'LC_ALL', 'TMPDIR', 'PATH')}
    env['PYTHONDONTWRITEBYTECODE'] = '1'
    p = subprocess.run(command, env=env, text=True, capture_output=True, timeout=30)
    if p.returncode:
        # Setup/import errors can contain paths; do not expose the caller's environment.
        raise RuntimeError(f'blocked:ros_environment_probe_failed:exit={p.returncode}')
    data = json.loads(p.stdout)
    child_env = dict(env)
    activation = data.pop('activation_environment')
    data['overlay_activation'] = {'kind': activation.get('R2F_OVERLAY_KIND'), 'source_repo': activation.get('R2F_OVERLAY_SOURCE')}
    child_env.update(activation)
    child_env['PYTHONPATH'] = str(REPO / 'src') + os.pathsep + child_env.get('PYTHONPATH', '')
    return data, child_env


def check_environment(config=None, *, probe_ports=True):
    config = config or {}
    repo = Path(config.get('source_repo', REPO)).resolve()
    old_repo = Path(config.get('old_repo', '/home/wuyuqa/ros2-dds-fuzz')).resolve()
    domain = config.get('domain_id', config.get('domain', 173))
    if isinstance(domain, bool) or not isinstance(domain, int) or not 0 <= domain <= 231:
        return {'ok': False, 'reason': 'blocked:domain_must_be_integer_0_to_231'}
    result = {'ok': False, 'environment_mode': 'ros_overlay', 'source': source_identity(repo),
              'domain_id': domain, 'target_revision': config.get('target_revision', 0),
              'setup': config.get('ros_setup', '/opt/ros/jazzy/setup.bash'),
              'overlay_setup': str(config.get('overlay_setup') or (REPO / 'scripts/setup_overlay.bash')), 'platform': sys.platform}
    overlay_path = Path(result['overlay_setup'])
    result['overlay'] = {'path': str(overlay_path), 'kind': 'python_source_overlay',
                         'sha256': hashlib.sha256(overlay_path.read_bytes()).hexdigest() if overlay_path.is_file() else None}
    result['whole_source_digest'] = config.get('source_digest', config.get('whole_source_digest'))
    setup_path = Path(result['setup'])
    result['underlay'] = {'path': str(setup_path), 'sha256': hashlib.sha256(setup_path.read_bytes()).hexdigest() if setup_path.is_file() else None}
    result['discarded_inherited_override_names'] = [name for name in ('CYCLONEDDS_URI', 'FASTRTPS_DEFAULT_PROFILES_FILE',
        'FASTDDS_DEFAULT_PROFILES_FILE', 'RMW_FASTRTPS_USE_QOS_FROM_XML', 'ROS_DISCOVERY_SERVER') if name in os.environ]
    result['process_scan'] = scan_processes(old_repo, domain)
    result['old_lock_scan'] = scan_old_locks(old_repo)
    if not result['process_scan']['ok'] or not result['old_lock_scan']['ok']:
        result['reason'] = 'domain_conflict:old_campaign_or_process_owner'
        return result
    if probe_ports:
        result['rtps_probe'] = probe_rtps_ports(domain)
        if not result['rtps_probe']['ok']:
            result['reason'] = 'domain_conflict:rtps_port_block'
            return result
    if config.get('environment_mode', 'ros_overlay') != 'ros_overlay':
        result['reason'] = 'blocked:environment_mode_not_ros_overlay'
        return result
    try:
        data, _ = activated_environment(config)
        result.update(data)
    except (OSError, RuntimeError, subprocess.SubprocessError, ValueError) as exc:
        result['reason'] = str(exc)
        return result
    if result.get('overlay_activation') != {'kind': 'python_source_overlay', 'source_repo': str(REPO)}:
        result['reason'] = 'blocked:new_repo_python_source_overlay_not_activated'
        return result
    if result.get('ros_distro') != 'jazzy':
        result['reason'] = 'blocked:ROS_DISTRO_mismatch'
        return result
    expected_head = config.get('expected_head')
    if expected_head and result['source']['head'] != expected_head:
        result['reason'] = 'blocked:source_HEAD_mismatch'
        return result
    if config.get('expected_target_revision', config.get('target_revision', 0)) != config.get('target_revision', 0):
        result['reason'] = 'blocked:target_revision_mismatch'
        return result
    if repo != REPO:
        result['reason'] = 'blocked:source_repo_mismatch'
        return result
    rmw = config.get('rmw')
    if rmw and not result.get('rmw', {}).get(rmw, {}).get('available'):
        result['reason'] = f'blocked:rmw_unavailable:{rmw}'
        return result
    if LOCK_FILE.is_file():
        lock = json.loads(LOCK_FILE.read_text())
        result['dependency_lock_sha256'] = hashlib.sha256(LOCK_FILE.read_bytes()).hexdigest()
        mismatch = []
        for name, pin in lock.get('packages', {}).items():
            actual = result['packages'].get(name, {})
            for key in ('version', 'library_sha256', 'package_xml_sha256'):
                if key in pin and pin[key] != actual.get(key):
                    mismatch.append(f'{name}:{key}')
        if mismatch:
            result['reason'] = 'blocked:dependency_identity_mismatch'
            result['dependency_mismatches'] = mismatch
            return result
    else:
        result['reason'] = 'blocked:dependencies_lock_missing'
        return result
    result['ok'] = True
    result['reason'] = None
    return result
