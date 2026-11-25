"""Opt-in native partition carrier; no endpoint or execution is created here.

Fast DDS Publisher/Subscriber QoS is a different API from RCL endpoint QoS.
The small compiled adapter borrows the latter's native endpoint under its held
handle. There is no loader override, interposition or instrumented SUT build.
"""
from __future__ import annotations
import ctypes as C
import hashlib
import json
import os
from pathlib import Path
import re
import time

KEY = 'native_matching_qos'
SCHEMA = 'r2f.native-matching-qos/v1'
BUILD_SCHEMA = 'r2f.native-matching-qos-build/v1'
SOURCE = Path(__file__).with_name('matching_qos_bridge.cpp')
CPP_RUNTIME = Path('/usr/lib/x86_64-linux-gnu/libstdc++.so.6').resolve()
CPP_ABI_EXPECTED = {'guid':'01.02.03.04.05.06.07.08.09.0a.0b.0c|00.00.01.02', 'stream_guid':'01.02.03.04.05.06.07.08.09.0a.0b.0c|0.0.1.2', 'finite_duration_sec':1.25}


def digest(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def reference(config):
    if KEY not in config:
        return None
    ref = config[KEY]
    if (not isinstance(ref, dict) or set(ref) != {'path', 'sha256'}
            or not isinstance(ref['path'], str) or not Path(ref['path']).is_absolute()
            or not isinstance(ref['sha256'], str) or not re.fullmatch('[0-9a-f]{64}', ref['sha256'])):
        raise ValueError('native_matching_qos_reference_invalid')
    if config.get('rmw') != 'rmw_fastrtps_cpp' or config.get('observation_mode', 'blackbox') != 'blackbox':
        raise ValueError('native_matching_qos_requires_fastdds_blackbox')
    return dict(ref)


def partitions(value):
    if (not isinstance(value, list) or len(value) > 64
            or any(not isinstance(s, str) or '\0' in s or len(s.encode('utf-8')) > 256 for s in value)):
        raise ValueError('native_partition_names_invalid')
    return list(value)


def load_build(ref):
    if digest(ref['path']) != ref['sha256']:
        raise ValueError('native_matching_qos_build_hash_mismatch')
    data = json.loads(Path(ref['path']).read_text())
    if (data.get('schema') != BUILD_SCHEMA or data.get('sut_modified') is not False
            or data.get('source_sha256') != digest(SOURCE)
            or data.get('cpp_abi_probe') != CPP_ABI_EXPECTED
            or not isinstance(data.get('files'), dict) or not data['files']
            or data.get('library') not in data['files']):
        raise ValueError('native_matching_qos_build_incomplete')
    required = [SOURCE.resolve(), SOURCE.with_name('native_string_sample.hpp').resolve(), CPP_RUNTIME,
                Path('/opt/ros/jazzy/lib/libfastcdr.so').resolve(), Path('/opt/ros/jazzy/lib/libfastrtps.so').resolve(),
                Path('/opt/ros/jazzy/lib/librmw_fastrtps_cpp.so').resolve()]
    if (any(str(path) not in data['files'] for path in required)
            or not isinstance(data.get('compiler_argv'), list) or not data['compiler_argv']
            or not data.get('compiler_version')):
        raise ValueError('native_matching_qos_build_provenance_missing')
    for path, expected in data['files'].items():
        if not Path(path).is_absolute() or digest(path) != expected:
            raise ValueError('native_matching_qos_dependency_changed:' + path)
    return data



def cpp_runtime_provenance(build):
    rows=[line.split(maxsplit=5) for line in Path('/proc/self/maps').read_text().splitlines()
          if len(line.split(maxsplit=5))==6
          and Path(line.split(maxsplit=5)[5]).name.startswith('libstdc++.so.')]
    if (not rows or any(Path(row[5]).resolve()!=CPP_RUNTIME or int(row[4])!=CPP_RUNTIME.stat().st_ino
                        for row in rows)):
        raise ValueError('native_matching_cpp_runtime_mapping_mismatch')
    return {'path':str(CPP_RUNTIME),'sha256':build['files'][str(CPP_RUNTIME)]}


def observe(endpoint, kind, requested, ref, *, apply, profile):
    """Set/read native parent QoS; copy everything before releasing the handle."""
    from .fastdds_peer_observer import verify_abi, _verify_api_maps
    verify_abi()
    maps = _verify_api_maps()
    build = load_build(ref)
    expected_vendor = Path('/opt/ros/jazzy/lib/libfastrtps.so').resolve()
    vendor_rows = [row.split(maxsplit=5) for row in Path('/proc/self/maps').read_text().splitlines()
                   if len(row.split(maxsplit=5)) == 6
                   and Path(row.split(maxsplit=5)[5]).name.startswith('libfastrtps.so')]
    if not vendor_rows or any(Path(row[5]).resolve() != expected_vendor for row in vendor_rows):
        raise ValueError('native_matching_qos_vendor_mapping_mismatch')
    requested = partitions(requested)
    lib = C.CDLL(build['library'])
    cpp_runtime=cpp_runtime_provenance(build)
    function = lib.r2f_matching_partitions
    function.argtypes = [C.c_void_p, C.c_int, C.c_int, C.c_size_t,
                         C.POINTER(C.c_char_p), C.c_void_p, C.c_size_t]
    function.restype = C.c_int
    encoded = (C.c_char_p * len(requested))(*(s.encode('utf-8') for s in requested))
    output = C.create_string_buffer(131072)
    handle = endpoint.handle
    with handle:
        code = function(handle.pointer, int(kind == 'reader'), int(apply), len(requested),
                        encoded, output, len(output))
        sampled = time.monotonic_ns()
        if code != 0:
            raise ValueError('native_matching_qos_bridge_error:' + str(code))
        result = json.loads(output.value)
    if result['return_code'] != 0 or result['after'] != requested:
        raise ValueError('native_partition_adoption_mismatch:' + str(result))
    return {'schema': SCHEMA, 'status': 'observed', 'api': 'FastDDS_Publisher_Subscriber_get_qos_set_qos',
            'scope': 'native_publisher_or_subscriber', 'application_requested': apply,
            'requested': requested, **result, 'pid': os.getpid(),
            'sampled_monotonic_ns': sampled, 'build_reference': dict(ref),
            'library': {'path': build['library'], 'sha256': build['files'][build['library']]},
            'mapped_api_libraries': maps, 'cpp_runtime':cpp_runtime, 'ownership_profile': dict(profile), 'sut_modified': False,
            'semantic_effect': 'not_inferred_from_configuration', 'candidate_eligible': False}


def validate_readback(created, requested, *, ownership='shared'):
    """Validate raw configuration identity; never use a declared name as a readback."""
    requested = partitions(requested)
    receipt = created.get('native_partition_qos')
    if receipt is None:
        if requested:
            raise ValueError('native_partition_readback_missing')
        return None
    if (not isinstance(receipt, dict) or receipt.get('schema') != SCHEMA
            or receipt.get('status') != 'observed' or (type(receipt.get('return_code')) is not int or receipt['return_code'] != 0)
            or receipt.get('api') != 'FastDDS_Publisher_Subscriber_get_qos_set_qos'
            or receipt.get('sut_modified') is not False
            or receipt.get('endpoint_gid') != created.get('endpoint_gid')
            or type(receipt.get('pid')) is not int or receipt['pid'] != created.get('pid')
            or type(receipt.get('sampled_monotonic_ns')) is not int
            or not 0 < receipt['sampled_monotonic_ns'] <= created.get('monotonic_ns', 0)
            or receipt.get('requested') != requested or receipt.get('after') != requested
            or receipt.get('ownership') != ownership):
        raise ValueError('native_partition_readback_binding_mismatch')
    partitions(receipt.get('before'))
    partitions(receipt.get('after'))
    for record in (receipt.get('build_reference'), receipt.get('library'), receipt.get('ownership_profile')):
        if (not isinstance(record, dict) or not isinstance(record.get('path'), str)
                or not Path(record['path']).is_absolute() or not isinstance(record.get('sha256'), str)
                or not re.fullmatch('[0-9a-f]{64}', record['sha256'])):
            raise ValueError('native_partition_readback_provenance_missing')
    return list(receipt['after'])


def literal_partition_compatibility(writer, reader):
    """Conservative DDS contract for vendor-independent literal partition names."""
    w, r = writer or [''], reader or ['']
    patterns = '*?[]\\'
    shared = {s for s in w if not any(ch in s for ch in patterns)} & set(r)
    if shared:
        return True
    if any(ch in name for name in w + r for ch in patterns):
        return None
    return False


def fastdds_partition_compatibility(writer, reader):
    """POSIX fnmatch/FNM_NOESCAPE contract of the pinned Linux Fast DDS API.

    These are independently observed native QoS names, not the vendor's match
    result. Pattern-to-pattern string matching is the API's two-direction test,
    not a claim that the languages denoted by two globs intersect.
    """
    import sys
    if sys.platform != 'linux':
        return literal_partition_compatibility(writer, reader)
    match = C.CDLL(None).fnmatch
    match.argtypes, match.restype = [C.c_char_p, C.c_char_p, C.c_int], C.c_int
    for offered in writer or ['']:
        for requested in reader or ['']:
            a, b = offered.encode('utf-8'), requested.encode('utf-8')
            if match(a, b, 2) == 0 or match(b, a, 2) == 0:
                return True
    return False


def audit_run(program, events, config, *, fixture):
    """Check selected build against every generation's persisted readbacks."""
    ref = reference(config)
    if ref is None:
        if any('native_partition_qos' in e for e in events):
            raise ValueError('native_partition_readback_without_selected_carrier')
        return None
    build = None if fixture else load_build(ref)
    declared = {ep['logical_id']: ep for ep in program['endpoints']}
    creates = [e for e in events if e.get('event') == 'endpoint_created']
    if not creates:
        raise ValueError('native_partition_endpoint_creation_missing')
    rows = []
    for event in creates:
        value = validate_readback(event, declared[event['logical_id']]['qos']['partitions'],
                                  ownership=declared[event['logical_id']]['qos']['ownership'])
        receipt = event.get('native_partition_qos')
        if value is None or receipt['build_reference'] != ref:
            raise ValueError('native_partition_selected_carrier_readback_missing_or_changed')
        if build and (receipt['library']['path'] != build['library']
                or receipt['library']['sha256'] != build['files'][build['library']]):
            raise ValueError('native_partition_selected_binary_mismatch')
        if build and receipt.get('cpp_runtime')!={'path':str(CPP_RUNTIME),'sha256':build['files'][str(CPP_RUNTIME)]}:
            raise ValueError('native_partition_cpp_runtime_provenance_missing_or_changed')
        profile = receipt['ownership_profile']
        if not fixture and (digest(profile['path']) != profile['sha256']
                or Path(profile['path']).read_bytes() != ownership_profile(
                    declared[event['logical_id']]['topic'], receipt['ownership'])):
            raise ValueError('native_ownership_profile_evidence_mismatch')
        rows.append({'logical_id': event['logical_id'], 'generation': event['generation'],
                     'endpoint_gid': event['endpoint_gid'], 'pid': event['pid'],
                     'event_index': event['event_index'], 'partitions': value, 'ownership': receipt['ownership']})
    return {'schema': 'r2f.matching-boundary-coverage/v1', 'build_reference': ref,
            'configuration_adoption': 'fixture' if fixture else 'observed', 'generations': rows,
            'coverage_rows': ['C10', 'C13', 'C15'], 'source_cases': [
                {'case_id': 'Fast-DDS-PR6336', 'classification': 'feature_extension_not_historical_bug',
                 'availability': (build or {}).get('feature_availability', {}).get('should_endpoints_match', 'not_checked'),
                 'normal_extension_execution': 'not_run', 'older_api_absence_is_sut_bug': False},
                {'case_id': 'OpenDDS/OpenDDS#2057', 'classification': 'upstream_report_exact_fix_unknown',
                 'historical_execution': 'not_run', 'reproduced_here': False}],
            'scope': 'existing_typed_ros_endpoints_native_partition_and_requested_offered_qos',
            'cross_module_obligations': ['qos_policy', 'type_contract', 'transport_delivery', 'local_lifecycle'],
            'attribution_does_not_remove_obligations': True,
            'candidate_eligible': False, 'required_denominator_credit': 0}


def ownership_profile(topic, ownership):
    """RMW loads these two topic profiles before creating its native endpoint."""
    import xml.etree.ElementTree as ET
    if ownership not in ('shared', 'exclusive'):
        raise ValueError('native_ownership_invalid')
    root = ET.Element('dds', xmlns='http://www.eprosima.com/XMLSchemas/fastRTPS_Profiles')
    profiles = ET.SubElement(root, 'profiles')
    for role in ('publisher', 'subscriber'):
        qos = ET.SubElement(ET.SubElement(profiles, role, profile_name=topic), 'qos')
        ET.SubElement(ET.SubElement(qos, 'ownership'), 'kind').text = ownership.upper()
    return ET.tostring(root, encoding='utf-8', xml_declaration=True)


def verify_profile(config):
    ref = config.get('native_qos_profile')
    expected = ownership_profile(config['topic'], config['native_ownership'])
    if (not isinstance(ref, dict) or digest(ref['path']) != ref['sha256']
            or Path(ref['path']).read_bytes() != expected
            or any(os.environ.get(key) != ref['path'] for key in
                   ('FASTRTPS_DEFAULT_PROFILES_FILE', 'FASTDDS_DEFAULT_PROFILES_FILE'))):
        raise ValueError('native_matching_qos_profile_binding_mismatch')


def pair_contract(writer, reader, writer_snapshot, reader_snapshot, declared):
    """Read native configuration independently; gaps do not erase other QoS facts."""
    tokens, snapshots = (writer, reader), (writer_snapshot, reader_snapshot)
    native = any('native_partition_qos' in token for token in tokens)
    native = native or any((declared[t['logical_id']].get('qos') or {}).get('partitions') for t in tokens)
    if not native:
        return None
    result = {'compatible': None, 'observation_gaps': [], 'responsibility_layer': 'qos_partition_matching',
              's1_coverage_rows': ['C10', 'C13', 'C15'], 'historical_validation': 'not_run',
              'historical_cases': ['Fast-DDS-PR6336', 'OpenDDS/OpenDDS#2057']}
    try:
        values, owners = [], []
        for token, snapshot in zip(tokens, snapshots):
            qos = declared[token['logical_id']]['qos']
            initial = validate_readback(token, qos['partitions'], ownership=qos['ownership'])
            if initial is None:
                raise ValueError('native_partition_pair_readback_incomplete')
            values.append(initial)
            owners.append(token['native_partition_qos']['ownership'])
            sample = snapshot.get('native_partition_qos')
            if sample is None:
                raise ValueError('native_partition_fresh_readback_missing')
            current = validate_readback(dict(token, monotonic_ns=snapshot['monotonic_ns'],
                native_partition_qos=sample), qos['partitions'], ownership=qos['ownership'])
            if (current != initial or sample['sampled_monotonic_ns'] < token['monotonic_ns']
                    or any(sample[key] != token['native_partition_qos'][key]
                           for key in ('build_reference', 'library', 'ownership_profile'))):
                raise ValueError('native_partition_fresh_readback_changed')
        compatible = fastdds_partition_compatibility(*values)
        result['partition'] = {'writer': values[0], 'reader': values[1], 'compatible': compatible,
            'basis': 'raw_native_parent_qos_not_ros_graph',
            'reason': 'posix_fnmatch_noescape_or_literal_match' if compatible else 'partition_mismatch'}
        result['ownership'] = {'writer': owners[0], 'reader': owners[1], 'compatible': owners[0] == owners[1]}
        result['compatible'] = compatible and owners[0] == owners[1]
    except (ValueError, KeyError, TypeError) as exc:
        result['observation_gaps'].append(str(exc))
    return result
