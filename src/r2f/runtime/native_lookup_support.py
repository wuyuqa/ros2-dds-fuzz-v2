"""Explicit generated native topics for typed lookup controls on a real participant.

These are actual DDS topic entities with XTypes, not evidence that an RMW topic
has TypeInformation. Loading descriptors creates no DDS entities; prepare/close
run only inside the canonical, source/lease-qualified worker. No ROS imports.
"""
from __future__ import annotations

import ctypes as c
import hashlib
import json
import os
from pathlib import Path
import subprocess


SCHEMA = 'r2f.native-lookup-support/v1'
PLACEMENTS = ('same_participant', 'other_participant', 'different_type', 'absent')
SOURCES = {
    'LookupTypes.idl': """// Explicit native lookup control types, not RMW TypeInformation or historical bindings.
module r2f_lookup {
  @final struct String { string data; };
  @final struct Dimension { string label; unsigned long size; unsigned long stride; };
  @final struct Layout { sequence<Dimension> dim; unsigned long data_offset; };
  @final struct UInt8MultiArray { Layout layout; sequence<octet> data; };
};
""",
    'native_lookup_descriptors.c': """/* Compiled against the same qualified Cyclone SDK as the worker. */
#include "LookupTypes.h"

const dds_topic_descriptor_t *r2f_lookup_descriptor(int array_type)
{
  return array_type ? &r2f_lookup_UInt8MultiArray_desc : &r2f_lookup_String_desc;
}
""",
}
PREFIX = Path('/opt/ros/jazzy')


def _sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def validate_reference(value):
    """Bounded deterministic input, validated before any artifact/library access."""
    return (isinstance(value, dict)
            and set(value) == {'manifest_path', 'manifest_sha256', 'placement'}
            and isinstance(value['manifest_path'], str)
            and 0 < len(value['manifest_path']) <= 4096
            and '\x00' not in value['manifest_path']
            and Path(value['manifest_path']).is_absolute()
            and isinstance(value['manifest_sha256'], str)
            and len(value['manifest_sha256']) == 64
            and all(ch in '0123456789abcdef' for ch in value['manifest_sha256'])
            and value['placement'] in PLACEMENTS)


def build_support(output, *, compiler, idlc=str(PREFIX / 'bin/idlc')):
    """Build descriptor-only support, never initialize or execute DDS/ROS."""
    output = Path(output).resolve()
    output.mkdir(parents=True, exist_ok=False)
    source = Path(__file__).resolve().parent
    from .topology_worker import CycloneMatchedPeerObserver
    from r2f.campaign.identity import source_manifest
    identity = source_manifest(source.parents[2])
    if identity.get('source_frozen') is not True:
        raise ValueError('native_lookup_build_source_not_frozen')
    if any(_sha(name) != digest for name, digest in CycloneMatchedPeerObserver.VERIFIED_C_HEADERS.items()):
        raise ValueError('native_lookup_build_sdk_changed')
    for name, text in SOURCES.items():
        (output / name).write_text(text)
    commands = [
        [str(Path(idlc).resolve()), '-f', 'case-sensitive', '-o', str(output), str(output / 'LookupTypes.idl')],
        [str(Path(compiler).resolve()), '-shared', '-fPIC', '-O2',
         '-I' + str(output), '-I' + str(PREFIX / 'include/CycloneDDS'),
         str(output / 'LookupTypes.c'), str(output / 'native_lookup_descriptors.c'),
         '-L' + str(PREFIX / 'lib/x86_64-linux-gnu'), '-lddsc',
         '-Wl,-rpath,' + str(PREFIX / 'lib/x86_64-linux-gnu'),
         '-o', str(output / 'liblookup_types.so')],
    ]
    logs = []
    for command in commands:
        result = subprocess.run(command, text=True, capture_output=True, timeout=120)
        logs.append({'command': command, 'returncode': result.returncode,
                     'stdout': result.stdout, 'stderr': result.stderr})
        (output / 'build-log.json').write_text(json.dumps(logs, indent=2) + '\n')
        if result.returncode:
            raise RuntimeError('native_lookup_support_build_failed:' + result.stderr)
    if source_manifest(source.parents[2]) != identity:
        raise ValueError('native_lookup_build_source_changed')
    manifest = {
        'schema': SCHEMA, 'source': identity,
        'source_sha256': {name: hashlib.sha256(text.encode()).hexdigest() for name, text in SOURCES.items()},
        'sdk_sha256': dict(CycloneMatchedPeerObserver.VERIFIED_C_HEADERS),
        'library_path': str(output / 'liblookup_types.so'),
        'library_sha256': _sha(output / 'liblookup_types.so'),
        'generated_sha256': {name: _sha(output / name)
                             for name in ('LookupTypes.c', 'LookupTypes.h')},
        'compiler': {'path': str(Path(compiler).resolve()), 'sha256': _sha(compiler)},
        'idlc': {'path': str(Path(idlc).resolve()), 'sha256': _sha(idlc)},
        'types': ['r2f_lookup::String', 'r2f_lookup::UInt8MultiArray'],
        'execution_evidence': False,
        'qualification': 'native_control_not_rmw_or_historical_binding',
    }
    path = output / 'manifest.json'
    path.write_text(json.dumps(manifest, indent=2) + '\n')
    return {'manifest_path': str(path), 'manifest_sha256': _sha(path)}


class NativeLookupControl:
    """Own only the explicit control topics and optional additional participant."""

    def __init__(self, observer, reference, ros_type_name):
        if not validate_reference(reference):
            raise ValueError('native_lookup_support_reference_invalid')
        if ros_type_name not in ('std_msgs/msg/String', 'std_msgs/msg/UInt8MultiArray'):
            raise ValueError('native_lookup_support_ros_schema_unknown')
        self.observer, self.reference = observer, dict(reference)
        self.array_type = ros_type_name == 'std_msgs/msg/UInt8MultiArray'
        self.owned = []
        self.receipt = {'reference': dict(reference), 'entities': [], 'cleanup': [],
                        'qualification': 'native_control_not_rmw_or_historical_binding',
                        'bound_ros_type_name': ros_type_name, 'candidate_eligible': False}
        path = Path(reference['manifest_path'])
        payload = path.read_bytes()
        if hashlib.sha256(payload).hexdigest() != reference['manifest_sha256']:
            raise ValueError('native_lookup_support_manifest_hash_mismatch')
        manifest = json.loads(payload)
        source = Path(__file__).resolve().parent
        if (manifest.get('schema') != SCHEMA
                or manifest.get('source_sha256') != {name: hashlib.sha256(text.encode()).hexdigest() for name, text in SOURCES.items()}
                or manifest.get('sdk_sha256') != observer.VERIFIED_C_HEADERS
                or any(_sha(name) != digest for name, digest in manifest['sdk_sha256'].items())
                or _sha(manifest['library_path']) != manifest.get('library_sha256')):
            raise ValueError('native_lookup_support_identity_mismatch')
        from r2f.campaign.identity import source_manifest
        if manifest.get('source') != source_manifest(source.parents[2]):
            raise ValueError('native_lookup_support_source_changed')
        self.receipt.update(library_path=manifest['library_path'],
                            library_sha256=manifest['library_sha256'],
                            manifest_source=manifest['source'])
        # The descriptor DSO calls no DDS API. The existing observer remains the
        # sole provider of runtime library/participant identity and functions.
        self.library = c.CDLL(manifest['library_path'], mode=os.RTLD_LOCAL)
        self.descriptor = self.library.r2f_lookup_descriptor
        self.descriptor.argtypes, self.descriptor.restype = [c.c_int], c.c_void_p

    def prepare(self, participant, domain, name):
        lib = self.observer.lib
        lib.dds_create_participant.argtypes = [c.c_uint32, c.c_void_p, c.c_void_p]
        lib.dds_create_participant.restype = c.c_int32
        lib.dds_create_topic.argtypes = [c.c_int32, c.c_void_p, c.c_char_p, c.c_void_p, c.c_void_p]
        lib.dds_create_topic.restype = c.c_int32
        self.source_topic = self._topic(participant,
            'r2f_lookup_source_' + hashlib.sha256(name.encode()).hexdigest()[:24], self.array_type)
        placement = self.reference['placement']
        if placement == 'absent':
            return self.source_topic
        owner = participant
        if placement == 'other_participant':
            owner = lib.dds_create_participant(domain, None, None)
            if owner <= 0:
                raise ValueError('native_lookup_control_participant_create:' + str(owner))
            self.owned.append(('participant', owner))
            guid = self.observer.Guid()
            rc = lib.dds_get_guid(owner, c.byref(guid))
            if rc != 0:
                raise ValueError('native_lookup_control_participant_guid:' + str(rc))
            self.receipt['sibling_participant'] = {'handle': owner, 'gid': bytes(guid.v).hex()}
        self._topic(owner, name, not self.array_type if placement == 'different_type' else self.array_type)
        return self.source_topic

    def _topic(self, owner, name, array_type):
        descriptor = self.descriptor(int(array_type))
        if not descriptor:
            raise ValueError('native_lookup_control_descriptor_unavailable')
        topic = self.observer.lib.dds_create_topic(owner, descriptor, name.encode(), None, None)
        if topic <= 0:
            raise ValueError('native_lookup_control_topic_create:' + str(topic))
        self.owned.append(('topic', topic))
        from .scoped_topic_lookup import _text_readback
        actual_name = _text_readback(c, self.observer.lib.dds_get_name, topic)
        actual_type = _text_readback(c, self.observer.lib.dds_get_type_name, topic)
        actual_owner = self.observer.lib.dds_get_participant(topic)
        expected_type = 'r2f_lookup::UInt8MultiArray' if array_type else 'r2f_lookup::String'
        row = {'handle': topic, 'name': actual_name, 'type_name': actual_type,
               'participant_handle': actual_owner, 'schema_source': 'LookupTypes.idl'}
        self.receipt['entities'].append(row)
        if (actual_name, actual_type, actual_owner) != (name, expected_type, owner):
            raise ValueError('native_lookup_control_entity_readback_mismatch')
        return topic

    def expected_status(self, scope):
        placement = self.reference['placement']
        visible = (placement == 'same_participant'
                   or placement == 'other_participant' and scope in ('local_domain', 'global'))
        return 'found' if visible else 'not_found'

    def close(self):
        complete = True
        for kind, entity in reversed(self.owned):
            row = {'kind': kind, 'handle': entity}
            try:
                row['return_code'] = self.observer.lib.dds_delete(entity)
                complete = complete and row['return_code'] == 0
            except (ValueError, TypeError, OSError, AttributeError, RuntimeError) as exc:
                row['error'] = str(exc)
                complete = False
            self.receipt['cleanup'].append(row)
        self.owned.clear()
        self.receipt['cleanup_confirmed'] = complete
        return complete


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', required=True)
    parser.add_argument('--cc', required=True)
    parser.add_argument('--idlc', default=str(PREFIX / 'bin/idlc'))
    args = parser.parse_args()
    print(json.dumps(build_support(args.output, compiler=args.cc, idlc=args.idlc), indent=2))
