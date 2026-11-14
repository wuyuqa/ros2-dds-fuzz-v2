"""CycloneDDS 0.10 discovery tracing, independent of rclpy application logs.

Configuration/ELF probes do not initialize DDS. Actual event evidence requires a
ROS endpoint process observed with the configured trace file open. Upstream source
references explain instrumentation points, not the distro binary's source commit.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass
from decimal import Decimal
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import time
from typing import Mapping
import xml.etree.ElementTree as ET

RMW = 'rmw_cyclonedds_cpp'
SCHEMA = 'r2f.cyclonedds-trace/v1'
SOURCE_REFERENCE = {
    'repository': 'https://github.com/eclipse-cyclonedds/cyclonedds',
    'tag': '0.10.5', 'path': 'src/core/ddsi/src/q_ddsi_discovery.c',
    'sha256': 'c1904e252f9be91ff3f716786fdcee3c6e7658be3d2751ec03697ec9fea77028',
    'binary_source_equivalence': 'unknown',
    'functions': ['handle_sedp', 'handle_sedp_alive_endpoint', 'handle_sedp_dead_endpoint'],
}
_HEADER = re.compile(r'^\s*(?P<time>\d{9,12}\.\d{6,9})\s+\[(?P<domain>\d*)\]\s+(?P<thread>[^:]+):\s*(?P<body>.*)$')
_GUID_TOKEN = r'(?:[0-9a-fA-F]{1,8}:){3}[0-9a-fA-F]{1,8}'
_SEDP = re.compile(r'\bSEDP ST(?P<status>[0-9a-fA-F]+)\s+(?P<guid>' + _GUID_TOKEN + r')(?:\s|$)')
_ALIVE = re.compile(
    r'\s(?P<reliability>reliable|best-effort)\s+'
    r'(?P<durability>volatile|transient-local|transient|persistent)\s+'
    r'(?P<kind>writer|reader)\s+.*?:\s+(?P<partition>.*?)\.'
    r'(?P<topic>[^\s]+)/(?P<type>[^\s]+)')
_QOS_FIELDS = ('reliability', 'durability', 'deadline', 'lifespan', 'liveliness',
               'liveliness_lease_duration', 'history', 'depth', 'ownership', 'destination_order')


def _stable_stat(stat):
    return (stat.st_dev, stat.st_ino, stat.st_size, stat.st_mtime_ns, stat.st_ctime_ns)


def file_sha256(path):
    digest = hashlib.sha256()
    with Path(path).open('rb') as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b''):
            digest.update(chunk)
    return digest.hexdigest()


def _tag(element):
    return element.tag.rsplit('}', 1)[-1]


@dataclass(frozen=True)
class TraceConfig:
    xml: str
    sha256: str
    output_file: str
    domain_id: int
    categories: tuple[str, ...] = ('discovery', 'plist')

    @property
    def environment(self):
        return {'CYCLONEDDS_URI': self.xml}

    def to_dict(self):
        return asdict(self)


def build_trace_config(output_file, *, domain_id, base_xml=None):
    """Preserve transport config, replace only Tracing; use one file per process.

    Inline XML avoids changing an already referenced file. Fresh output is required
    by the collector/probe owner, and '$' is rejected because Cyclone expands env
    variables in config values. Multiple Domain sections are deliberately rejected.
    """
    if isinstance(domain_id, bool) or not isinstance(domain_id, int) or not 0 <= domain_id <= 231:
        raise ValueError('domain_id must be an integer in 0..231')
    output = Path(output_file)
    if not output.is_absolute() or any(c in str(output) for c in ('$','\n','\r','\x00')):
        raise ValueError('trace output must be an absolute path without variable expansion')
    if output.exists():
        raise FileExistsError(output)
    root = ET.fromstring(base_xml) if base_xml else ET.Element('CycloneDDS')
    if _tag(root) != 'CycloneDDS':
        raise ValueError('base_xml root must be CycloneDDS')
    ns = root.tag[:-len('CycloneDDS')]
    domains = [e for e in root if _tag(e) == 'Domain']
    if len(domains) > 1:
        raise ValueError('multiple Domain configurations require explicit normalization')
    domain = domains[0] if domains else ET.SubElement(root, ns + 'Domain', {'Id': str(domain_id)})
    if domain.get('Id', domain.get('id', 'any')) not in ('any', str(domain_id)):
        raise ValueError('base Domain does not apply to requested domain_id')
    if any(_tag(e) == 'Tracing' for e in root):
        raise ValueError('legacy root-level Tracing must be normalized first')
    for e in list(domain):
        if _tag(e) == 'Tracing':
            domain.remove(e)
    tracing = ET.SubElement(domain, ns + 'Tracing')
    for name, value in (('Verbosity', 'none'), ('Category', 'discovery,plist'),
                        ('OutputFile', str(output)), ('AppendToFile', 'false')):
        ET.SubElement(tracing, ns + name).text = value
    xml = ET.tostring(root, encoding='unicode', short_empty_elements=True)
    return TraceConfig(xml, hashlib.sha256(xml.encode()).hexdigest(), str(output), domain_id)


def environment_for_mode(base_env: Mapping[str, str], *, mode, trace_config=None,
                         instrumentation_roots=()):
    """Never mutate os.environ. Blackbox always strips CYCLONEDDS_URI.

    Runtime supplies a freshly activated ROS overlay environment. Preload/audit
    hooks and instrumentation path components must not leak into a blackbox run.
    The caller must separately choose/verify its executable, as env cleaning alone
    cannot turn an instrumented executable into an uninstrumented binary.
    """
    if mode not in ('blackbox', 'instrumentation'):
        raise ValueError('mode must be blackbox or instrumentation')
    env = dict(base_env)
    roots = [Path(p).resolve() for p in instrumentation_roots]
    for key in list(env):
        if key == 'CYCLONEDDS_URI' or key.startswith(('R2F_INSTRUMENT', 'CYCLONEDDS_TRACE')) or key in ('LD_PRELOAD', 'LD_AUDIT'):
            env.pop(key)
    for key in ('PATH', 'LD_LIBRARY_PATH', 'PYTHONPATH', 'AMENT_PREFIX_PATH', 'CMAKE_PREFIX_PATH', 'COLCON_PREFIX_PATH'):
        if key in env and roots:
            components = [p for p in env[key].split(os.pathsep) if p and not any(Path(p).resolve().is_relative_to(r) for r in roots)]
            env[key] = os.pathsep.join(components)
    if mode == 'instrumentation':
        if not isinstance(trace_config, TraceConfig):
            raise ValueError('instrumentation requires an explicit TraceConfig')
        env.update(trace_config.environment)
    elif trace_config is not None:
        raise ValueError('blackbox must not receive a TraceConfig')
    return env


def normalize_guid(value):
    """Cyclone PGUID words or rmw_cyclonedds graph GID (16/24 bytes).

    rmw 2.2.4 convert_guid_to_gid copies dds_guid_t.v then zero pads. Delivery
    MessageInfo.publisher_gid is not interchangeable with graph endpoint_gid.
    """
    if isinstance(value, str):
        if re.fullmatch(_GUID_TOKEN, value):
            return ''.join(f'{int(word, 16):08x}' for word in value.split(':'))
        compact = value.replace('.', '').replace('-', '')
        if re.fullmatch(r'[0-9a-fA-F]{32}', compact):
            return compact.lower()
        raise ValueError('not a Cyclone endpoint GUID')
    raw = bytes(value)
    if len(raw) not in (16, 24) or any(raw[16:]):
        raise ValueError('graph endpoint GID must be 16 bytes or 24 bytes with zero padding')
    return raw[:16].hex()


def _parse_qos(raw):
    """Extract actual logged policies; do not synthesize omitted defaults.

    Top-level tokenization avoids treating user_data strings/nested collections
    as policy names. Integer duration values are vendor nanoseconds, including
    DDS infinity (INT64_MAX); graph adapters must use the same representation.
    """
    if raw is None:
        return {}
    fields, chunk, stack, quoted, escaped = [], [], [], False, False
    pairs = {'{': '}', '<': '>', '(': ')', '[': ']'}
    for char in raw:
        if quoted:
            chunk.append(char)
            if escaped:
                escaped = False
            elif char == '\\':
                escaped = True
            elif char == '"':
                quoted = False
            continue
        if char == '"':
            quoted = True
        elif char in pairs:
            stack.append(pairs[char])
        elif stack and char == stack[-1]:
            stack.pop()
        elif char == ',' and not stack:
            fields.append(''.join(chunk))
            chunk = []
            continue
        chunk.append(char)
    fields.append(''.join(chunk))
    values = {}
    for field in fields:
        key, equals, value = field.partition('=')
        if equals:
            values[key.strip()] = value.strip()
    result = {}
    for key in ('deadline', 'lifespan'):
        if re.fullmatch(r'-?\d+', values.get(key, '')):
            result[key] = int(values[key])
    mappings = {
        'ownership': {0: 'shared', 1: 'exclusive'},
        'destination_order': {0: 'by_reception_timestamp', 1: 'by_source_timestamp'},
    }
    for key, mapping in mappings.items():
        if values.get(key, '').isdigit() and int(values[key]) in mapping:
            result[key] = mapping[int(values[key])]
    history = re.fullmatch(r'(\d+):(-?\d+)', values.get('history', ''))
    if history and int(history[1]) in (0, 1):
        result['history'] = {0: 'keep_last', 1: 'keep_all'}[int(history[1])]
        if int(history[2]) >= 0:
            result['depth'] = int(history[2])
    liveliness = re.fullmatch(r'(\d+):(-?\d+)', values.get('liveliness', ''))
    if liveliness and int(liveliness[1]) in (0, 1, 2):
        result['liveliness'] = {0: 'automatic', 1: 'manual_by_participant', 2: 'manual_by_topic'}[int(liveliness[1])]
        result['liveliness_lease_duration'] = int(liveliness[2])
    return result


def parse_trace(data: bytes | str):
    """Parse SEDP receives only. No graph/application event promotion.

    Input is unverified until collect_trace attaches process/file provenance.
    Timestamp is vendor wall-clock log time, not DDS sample/source timestamp.
    Unknown policies are never copied from graph or assumed from ROS defaults.
    """
    raw = data.encode() if isinstance(data, str) else data
    events, diagnostics = [], []
    offset = 0
    for number, line_bytes in enumerate(raw.splitlines(keepends=True), 1):
        line = line_bytes.decode('utf-8', errors='replace').rstrip('\r\n')
        header = _HEADER.match(line)
        match = _SEDP.search(header['body']) if header else None
        if match:
            status = int(match['status'], 16)
            body = header['body']
            guid = normalize_guid(match['guid'])
            entity_kind = int(guid[-2:], 16)
            kind = 'writer' if entity_kind in (2, 3) else 'reader' if entity_kind in (4, 7) else None
            alive = _ALIVE.search(body) if not status & 3 else None
            qos = {key: None for key in _QOS_FIELDS}
            if alive:
                qos.update(reliability=alive['reliability'].replace('-', '_'),
                           durability=alive['durability'].replace('-', '_'))
                kind = alive['kind']
            qos_raw = body.split('QOS={', 1)[1].rsplit('}', 1)[0] if 'QOS={' in body else None
            qos.update(_parse_qos(qos_raw))
            event = {
                'event': 'sedp_endpoint_disposed' if status & 3 else 'sedp_endpoint_alive',
                'source': 'vendor_internal_trace', 'provenance_status': 'unverified_input',
                'instrumentation_point': 'handle_sedp_dead_endpoint' if status & 3 else 'handle_sedp_alive_endpoint',
                'event_time_ns': int(Decimal(header['time']) * 1_000_000_000),
                'clock': 'unix_wall_clock_vendor_log', 'timestamp_precision_ns': 10 ** (9 - len(header['time'].split('.')[1])),
                'domain_id': int(header['domain']) if header['domain'] else None,
                'thread': header['thread'].strip(), 'endpoint_guid': guid,
                'endpoint_kind': kind, 'statusinfo': status,
                'topic_name': alive['topic'] if alive else None,
                'type_name': alive['type'] if alive else None,
                'qos': qos, 'qos_raw': qos_raw,
                'qos_scope': 'vendor_effective_received_sedp_qos_not_explicit_wire_policy_presence',
                'unknown_fields': [key for key, value in qos.items() if value is None],
                'raw_line': number, 'raw_byte_offset': offset, 'raw_byte_length': len(line_bytes),
                'raw_line_sha256': hashlib.sha256(line_bytes).hexdigest(),
            }
            if not status & 3 and not alive:
                event['unknown_fields'] += ['topic_name', 'type_name']
                diagnostics.append({'line': number, 'reason': 'incomplete_or_unrecognized_sedp_alive'})
            events.append(event)
        offset += len(line_bytes)
    return {'schema': SCHEMA, 'raw_sha256': hashlib.sha256(raw).hexdigest(),
            'events': events, 'diagnostics': diagnostics, 'source_reference': SOURCE_REFERENCE}


def capture_process_provenance(pid, *, trace_config=None, proc_root=Path('/proc')):
    """Read a live process's maps/environ/fds; never call DDS or inspect secrets."""
    root = Path(proc_root) / str(int(pid))
    result = {'pid': int(pid), 'captured_at_unix_ns': time.time_ns(), 'loaded_libraries': [],
              'unknown': [], 'scope': 'observed_process', 'trace_fd_attached': False}
    try:
        stat_before = (root / 'stat').read_text()
        result['process_start_ticks'] = stat_before.rsplit(')', 1)[1].split()[19]
        result['process_group'] = int(stat_before.rsplit(')', 1)[1].split()[2])
        result['exe'] = str((root / 'exe').resolve(strict=True))
        selected = {}
        for entry in (root / 'environ').read_bytes().split(b'\0'):
            key, _, value = entry.partition(b'=')
            if key in (b'CYCLONEDDS_URI', b'RMW_IMPLEMENTATION', b'ROS_DOMAIN_ID'):
                selected[key.decode()] = value.decode(errors='replace')
        result['selected_environment'] = selected
        seen = set()
        for line in (root / 'maps').read_text().splitlines():
            fields = line.split(maxsplit=5)
            if len(fields) != 6:
                continue
            name = fields[5]
            if not re.search(r'/lib(?:ddsc|rmw_cyclonedds_cpp)\.so', name) or name in seen:
                continue
            seen.add(name)
            record = {'mapped_path': name, 'mapped_inode': int(fields[4])}
            try:
                path = Path(name).resolve(strict=True)
                before = path.stat()
                record.update(path=str(path), sha256=file_sha256(path), size=before.st_size)
                after = path.stat()
                record['identity_verified'] = (before.st_ino == int(fields[4]) and _stable_stat(before) == _stable_stat(after)
                                                and f'{os.major(before.st_dev):02x}:{os.minor(before.st_dev):02x}' == fields[3])
            except OSError as exc:
                record.update(identity_verified=False, error=type(exc).__name__)
            result['loaded_libraries'].append(record)
        if trace_config is not None:
            result['config_matches'] = selected.get('CYCLONEDDS_URI') == trace_config.xml
            result['domain_matches'] = selected.get('ROS_DOMAIN_ID') == str(trace_config.domain_id)
            result['rmw_matches'] = selected.get('RMW_IMPLEMENTATION') == RMW
            expected = Path(trace_config.output_file).resolve()
            for fd in (root / 'fd').iterdir():
                try:
                    if fd.resolve(strict=True) == expected:
                        result['trace_fd_attached'] = True
                        result['trace_fd'] = fd.name
                        result['trace_inode'] = expected.stat().st_ino
                except OSError:
                    continue
        stat_after = (root / 'stat').read_text()
        result['process_identity_stable'] = (stat_before.rsplit(')', 1)[1].split()[19]
                                             == stat_after.rsplit(')', 1)[1].split()[19])
    except (OSError, ValueError, IndexError) as exc:
        result['unknown'].append(type(exc).__name__ + ':' + str(exc))
        result['process_identity_stable'] = False
    result['vendor_loaded'] = all(any(lib.get('identity_verified') and token in lib['mapped_path']
                                               for lib in result['loaded_libraries'])
                                  for token in ('/libddsc.so', '/librmw_cyclonedds_cpp.so'))
    return result


def probe_cyclonedds(ros_prefix='/opt/ros/jazzy'):
    """Load the installed RMW in a short child process; creates no participant."""
    prefix = Path(ros_prefix).resolve()
    header = prefix / 'include/CycloneDDS/dds/version.h'
    cfg = prefix / 'include/CycloneDDS/dds/ddsi/ddsi_cfgelems.h'
    package = prefix / 'share/rmw_cyclonedds_cpp/package.xml'
    result = {'schema': SCHEMA, 'scope': 'loader_probe_only', 'participant_created': False,
              'captured_at_unix_ns': time.time_ns(), 'ros_prefix': str(prefix), 'available': False}
    try:
        result['cyclonedds_version'] = re.search(r'#define DDS_VERSION "([^"]+)"', header.read_text())[1]
        result['rmw_version'] = ET.parse(package).getroot().findtext('version')
        result['installed_config_header'] = {'path': str(cfg), 'sha256': file_sha256(cfg)}
        result['installed_version_header'] = {'path': str(header), 'sha256': file_sha256(header)}
        text = cfg.read_text()
        result['tracing_config_supported'] = all(token in text for token in ('"discovery"', '"plist"', '"OutputFile"', '"AppendToFile"'))
        code = ('import ctypes,json,os; from r2f.instrumentation.cyclonedds import capture_process_provenance;'
                'ctypes.CDLL(' + repr(str(prefix / 'lib/librmw_cyclonedds_cpp.so')) + ');'
                'print(json.dumps(capture_process_provenance(os.getpid())))')
        env = {key: value for key, value in os.environ.items() if key in ('HOME', 'USER', 'LANG', 'PATH', 'TMPDIR')}
        env['PYTHONDONTWRITEBYTECODE'] = '1'
        command = ['bash', '--noprofile', '--norc', '-c',
                   'source "$1" >/dev/null && export PYTHONPATH="$2${PYTHONPATH:+:$PYTHONPATH}" && exec /usr/bin/python3 -B -c "$3"',
                   'r2f-vendor-loader-probe', str(prefix / 'setup.bash'), str(Path(__file__).resolve().parents[2]), code]
        completed = subprocess.run(command, env=env, capture_output=True, text=True, timeout=25)
        result['probe_exit_code'] = completed.returncode
        if completed.returncode:
            result['error'] = completed.stderr[-2000:]
        else:
            result['loaded_provenance'] = json.loads(completed.stdout)
            result['loaded_provenance']['scope'] = 'loader_probe_only'
            result['available'] = result['loaded_provenance']['vendor_loaded'] and result['tracing_config_supported']
        versions = subprocess.run(['dpkg-query', '-W', '-f=${Package}\t${Version}\n',
                                   'ros-jazzy-cyclonedds', 'ros-jazzy-rmw-cyclonedds-cpp'],
                                  capture_output=True, text=True, timeout=10)
        result['deb_packages'] = versions.stdout.splitlines()
    except (OSError, ValueError, ET.ParseError, TypeError, subprocess.SubprocessError) as exc:
        result['error'] = type(exc).__name__ + ':' + str(exc)
    return result


def collect_trace(config, *, processes, control_task):
    """Collect a closed trace file and attach actual endpoint writer provenance.

    The caller must stop its workers first, while supplying fd/maps captures taken
    while they were alive. A loader probe is explicitly insufficient. Collection
    does not make a candidate or decide SUT responsibility.
    """
    if not isinstance(config, TraceConfig):
        raise ValueError('config must be TraceConfig')
    path = Path(config.output_file)
    before = path.stat()
    raw = path.read_bytes()
    after = path.stat()
    if _stable_stat(before) != _stable_stat(after):
        raise ValueError('trace changed during collection')
    result = parse_trace(raw)
    eligible = [p for p in processes if p.get('scope') == 'observed_process'
                and p.get('vendor_loaded') and p.get('process_identity_stable')
                and p.get('config_matches') and p.get('domain_matches') and p.get('rmw_matches')
                and p.get('trace_fd_attached') and p.get('trace_inode') == before.st_ino]
    verified = len(eligible) == 1
    for event in result['events']:
        event['provenance_status'] = 'verified_vendor_process' if verified else 'unknown'
        event['raw_path'] = str(path)
        event['raw_sha256'] = result['raw_sha256']
        if verified:
            event['observer_pid'] = eligible[0]['pid']
    result.update(raw_path=str(path), config=config.to_dict(), processes=processes,
                  control_task=control_task, blackbox_validation='not_run',
                  vendor_process_verified=verified,
                  internal_event_witness=verified and bool(result['events']),
                  evidence_tier='instrumented' if verified else None,
                  source_commit=None, patch_digest=None, compiler_flags=None,
                  build_provenance='unmodified_installed_vendor_binary; source commit/compiler flags unknown',
                  candidate_status='none', candidate_admission='not_performed',
                  collection_time_unix_ns=time.time_ns())
    return result
