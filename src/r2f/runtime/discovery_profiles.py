"""Explicit loopback UDP profiles, identical across third-target control arms."""
from __future__ import annotations
import hashlib
from pathlib import Path

CYCLONE_XML = '''<?xml version="1.0" encoding="UTF-8"?>
<CycloneDDS xmlns="https://cdds.io/config"><Domain Id="any">
<General><Transport>udp</Transport><Interfaces><NetworkInterface name="lo" multicast="true"/></Interfaces>
<AllowMulticast>true</AllowMulticast><EnableMulticastLoopback>true</EnableMulticastLoopback></General>
<Discovery><ParticipantIndex>auto</ParticipantIndex><MaxAutoParticipantIndex>31</MaxAutoParticipantIndex>
<SPDPInterval>1s</SPDPInterval></Discovery></Domain></CycloneDDS>
'''
FAST_XML = '''<?xml version="1.0" encoding="UTF-8"?>
<dds xmlns="http://www.eprosima.com/XMLSchemas/fastRTPS_Profiles"><profiles>
<transport_descriptors><transport_descriptor><transport_id>r2f_loopback_udp</transport_id><type>UDPv4</type>
<interfaceWhiteList><address>127.0.0.1</address></interfaceWhiteList></transport_descriptor></transport_descriptors>
<participant profile_name="r2f_{role}" is_default_profile="true"><rtps><participantID>{index}</participantID>
<builtin><discovery_config><discoveryProtocol>SIMPLE</discoveryProtocol>
<leaseDuration><sec>20</sec><nanosec>0</nanosec></leaseDuration>
<leaseAnnouncement><sec>1</sec><nanosec>0</nanosec></leaseAnnouncement></discovery_config></builtin>
<userTransports><transport_id>r2f_loopback_udp</transport_id></userTransports><useBuiltinTransports>false</useBuiltinTransports>
</rtps></participant></profiles></dds>
'''


def validate_discovery_layers(value, rmw):
    if value != 'participant_topic_v1':
        raise ValueError('unsupported_projection_discovery_layers')
    if rmw != 'rmw_cyclonedds_cpp':
        raise ValueError('unsupported_projection_discovery_layers_rmw')


def cyclone_profile(discovery_layers=None):
    if discovery_layers is None:
        return CYCLONE_XML
    validate_discovery_layers(discovery_layers, 'rmw_cyclonedds_cpp')
    return CYCLONE_XML.replace('<Discovery>',
        '<Discovery><EnableTopicDiscoveryEndpoints>true</EnableTopicDiscoveryEndpoints>')


def write_profiles(output_dir, rmw, *, discovery_layers=None):
    if discovery_layers is not None:
        validate_discovery_layers(discovery_layers, rmw)
    root = Path(output_dir) / 'profiles'
    root.mkdir()
    records, environments = {}, {}
    roles = ('writer', 'reader', 'builtin_observer') if discovery_layers else ('writer', 'reader')
    for index, role in enumerate(roles):
        text = cyclone_profile(discovery_layers) if rmw == 'rmw_cyclonedds_cpp' else FAST_XML.format(role=role, index=index)
        path = root / (role + '.xml')
        with path.open('x') as handle:
            handle.write(text)
        records[role] = {'path': str(path), 'sha256': hashlib.sha256(text.encode()).hexdigest(),
                         'kind': 'explicit_loopback_udp_profile', 'participant_index': 'auto' if rmw == 'rmw_cyclonedds_cpp' else index}
        key = 'CYCLONEDDS_URI' if rmw == 'rmw_cyclonedds_cpp' else 'FASTRTPS_DEFAULT_PROFILES_FILE'
        environments[role] = {key: str(path)}
    return records, environments
