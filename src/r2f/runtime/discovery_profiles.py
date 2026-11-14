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


def write_profiles(output_dir, rmw):
    root = Path(output_dir) / 'profiles'
    root.mkdir()
    records, environments = {}, {}
    for index, role in enumerate(('writer', 'reader')):
        text = CYCLONE_XML if rmw == 'rmw_cyclonedds_cpp' else FAST_XML.format(role=role, index=index)
        path = root / (role + '.xml')
        with path.open('x') as handle:
            handle.write(text)
        records[role] = {'path': str(path), 'sha256': hashlib.sha256(text.encode()).hexdigest(),
                         'kind': 'explicit_loopback_udp_profile', 'participant_index': 'auto' if rmw == 'rmw_cyclonedds_cpp' else index}
        key = 'CYCLONEDDS_URI' if rmw == 'rmw_cyclonedds_cpp' else 'FASTRTPS_DEFAULT_PROFILES_FILE'
        environments[role] = {key: str(path)}
    return records, environments
