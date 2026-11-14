"""Read-only Fast DDS attached-reader/discovery feasibility, no DDS entities.

A declared builtin struct is not an observation. Inventory distinguishes an
unsupported public getter from a possible observer on the RMW's existing
participant discovery callbacks. No listeners are replaced or installed here.
"""
from pathlib import Path
import re
import struct
import subprocess
import time

from .cyclonedds import file_sha256


def _symbol_stubs(path, needles):
    """Inspect ELF64 LE dynsym/code bytes without executing native methods."""
    data = Path(path).read_bytes()
    if data[:6] != b'\x7fELF\x02\x01':
        return {'status': 'unsupported_elf_format'}
    machine = struct.unpack_from('<H', data, 18)[0]
    offset = struct.unpack_from('<Q', data, 40)[0]
    entry_size, count = struct.unpack_from('<HH', data, 58)
    sections = [struct.unpack_from('<IIQQQQIIQQ', data, offset + entry_size * i) for i in range(count)]
    result = {'machine': machine, 'inspection': 'static_binary_bytes_not_a_method_call', 'symbols': []}
    for section in sections:
        if section[1] != 11:
            continue
        strings = sections[section[6]]
        names = data[strings[4]: strings[4] + strings[5]]
        for pos in range(section[4], section[4] + section[5], section[9]):
            name_offset, info, other, index, value, size = struct.unpack_from('<IBBHQQ', data, pos)
            name = names[name_offset:names.find(b'\0', name_offset)].decode(errors='replace')
            if not any(needle in name for needle in needles) or not 0 < index < len(sections):
                continue
            target = sections[index]
            code_offset = target[4] + value - target[3]
            code = data[code_offset:code_offset + min(size, 32)]
            result['symbols'].append({'mangled_symbol': name, 'size': size, 'code_hex': code.hex(),
                                      'returns_unsupported_stub': machine == 62 and size <= 16
                                      and code.endswith(bytes.fromhex('b802000000c3'))})
    return result


def probe_fastdds(ros_prefix='/opt/ros/jazzy'):
    prefix = Path(ros_prefix).resolve()
    includes = prefix / 'include'
    paths = {
        'version': includes / 'fastrtps/fastrtps/config.h',
        'reader': includes / 'fastrtps/fastdds/dds/subscriber/DataReader.hpp',
        'reader_listener': includes / 'fastrtps/fastdds/dds/subscriber/DataReaderListener.hpp',
        'builtin_publication': includes / 'fastrtps/fastdds/dds/builtin/topic/PublicationBuiltinTopicData.hpp',
        'attached_reader_getter': includes / 'rmw_fastrtps_cpp/rmw_fastrtps_cpp/get_subscriber.hpp',
        'rmw_participant_listener': includes / 'rmw_fastrtps_shared_cpp/rmw_fastrtps_shared_cpp/custom_participant_info.hpp',
        'writer_proxy': includes / 'fastrtps/fastdds/rtps/builtin/data/WriterProxyData.h',
        'writer_proxy_qos': includes / 'fastrtps/fastdds/dds/publisher/qos/WriterQos.hpp',
    }
    result = {'schema': 'r2f.fastdds-discovery-feasibility/v1', 'scope': 'static_installed_headers_and_binary',
              'captured_at_unix_ns': time.time_ns(), 'participant_created': False,
              'attached_reader_method_called': False, 'listener_modified': False,
              'candidate_admission': 'not_performed', 'target_maturity': 'not_evaluated',
              'headers': {}, 'unknown': [], 'observation_implemented': False}
    texts = {}
    for name, path in paths.items():
        try:
            texts[name] = path.read_text()
            result['headers'][name] = {'path': str(path), 'sha256': file_sha256(path)}
        except OSError:
            result['unknown'].append('header_missing:' + name)
    version = re.search(r'#define FASTRTPS_VERSION_STR "([^"]+)"', texts.get('version', ''))
    result['fastdds_version'] = version[1] if version else None
    versions = subprocess.run(['dpkg-query', '-W', '-f=${Package}\t${Version}\n',
                               'ros-jazzy-fastrtps', 'ros-jazzy-rmw-fastrtps-cpp',
                               'ros-jazzy-rmw-fastrtps-shared-cpp'],
                              capture_output=True, text=True, timeout=10)
    result['deb_packages'] = versions.stdout.splitlines()
    library = prefix / 'lib/libfastrtps.so'
    if library.is_file():
        library = library.resolve()
        result['library'] = {'path': str(library), 'sha256': file_sha256(library),
                             'provenance_scope': 'installed_file_not_loaded_endpoint'}
        result['public_getter_binary'] = _symbol_stubs(library, ('get_matched_publication_data', 'get_matched_publications'))
    result['attached_reader_access'] = {
        'declared': 'get_datareader(rmw_subscription_t * subscription)' in texts.get('attached_reader_getter', ''),
        'api': 'rmw_fastrtps_cpp::get_datareader(existing_rmw_subscription)',
        'creates_participant': False,
        'rclpy_bridge': 'not_implemented; requires existing rcl/rmw subscription handle bridge',
    }
    result['matched_publication_path'] = {
        'status': 'blocked:public_api_unsupported' if all(
            s['returns_unsupported_stub'] for s in result.get('public_getter_binary', {}).get('symbols', []))
            and len(result.get('public_getter_binary', {}).get('symbols', [])) >= 2 else 'unknown',
        'header_basis': 'DataReader.hpp documents RETCODE_UNSUPPORTED for both getters',
        'method_call_observed': False,
        'unknown_fields': ['endpoint_guid', 'topic_name', 'type_name', 'reliability', 'durability', 'depth', 'deadline', 'liveliness'],
    }
    result['reader_listener_path'] = {
        'on_data_available': 'user topic data notification; not an independent builtin discovery observation',
        'on_subscription_matched': 'matched status/handle only; unavailable getter does not supply builtin QoS',
        'discovery_callback_declared_on_datareader_listener': 'on_publisher_discovery' in texts.get('reader_listener', ''),
        'status': 'insufficient_builtin_fields',
    }
    qos_text = texts.get('writer_proxy_qos', '')
    result['existing_participant_listener_path'] = {
        'status': 'feasible_instrumentation_point_not_implemented',
        'points': ['ParticipantListener::on_publisher_discovery(WriterDiscoveryInfo&&)',
                   'ParticipantListener::on_subscriber_discovery(ReaderDiscoveryInfo&&)'],
        'existing_rmw_callbacks_declared': all(x in texts.get('rmw_participant_listener', '') for x in
                                              ('on_publisher_discovery', 'on_subscriber_discovery', 'proxyData.m_qos')),
        'available_in_proxy_schema': ['guid', 'topicName', 'typeName'] +
            [key for key, token in (('reliability', 'm_reliability'), ('durability', 'm_durability'),
                                   ('deadline', 'm_deadline'), ('liveliness', 'm_liveliness')) if token in qos_text],
        'depth': 'unknown; WriterQos and PublicationBuiltinTopicData have no HistoryQosPolicy/depth member',
        'independence_requirement': 'capture existing discovery proxy data before graph cache projection; graph values cannot fill missing fields',
        'attachment_requirement': 'observe existing RMW callbacks without replacing listener/creating participant',
        'limitation': 'no dynamic attachment or vendor trace containing these fields has been demonstrated',
    }
    result['next_path'] = 'read-only trace of existing discovery callbacks or passive normal SEDP in isolated netns; retain missing policy presence as unknown'
    return result
