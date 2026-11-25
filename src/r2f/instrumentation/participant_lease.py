"""Participant discovery leases: supporting facts, never writer liveliness.

Cyclone's vendor trace is an internal-path witness, not proof of a race or
binary/source equivalence. Every receive incarnation is scoped to its observing
process and remote GUID; local ROS endpoint generations are retained separately.
"""
from decimal import Decimal
import hashlib
import re

from .cyclonedds import _HEADER, _GUID_TOKEN, normalize_guid

SOURCE_REFERENCES = [
    {'tag': '0.10.5', 'path': 'src/core/ddsi/src/q_ddsi_discovery.c',
     'sha256': 'c1904e252f9be91ff3f716786fdcee3c6e7658be3d2751ec03697ec9fea77028'},
    {'tag': '0.10.5', 'path': 'src/core/ddsi/src/q_lease.c',
     'sha256': '236fdb3aff0b7355c7dc171298616293506acf4359d1de934889f97b28a46ddc'},
    {'tag': '0.10.5', 'path': 'src/core/ddsi/src/ddsi_proxy_participant.c',
     'sha256': '737cdbd87ad956af01e6e682773ef17c185e06ce9b7ecd368dbc142f70a6cf8d'},
]
_SPDP = re.compile(r'\bSPDP ST0 (?P<guid>' + _GUID_TOKEN + r')(?P<detail>.*)')
_EXPIRED = re.compile(r'\blease expired: l \S+ guid (?P<guid>' + _GUID_TOKEN +
                      r') tend (?P<deadline>\d+) < now (?P<now>\d+)')
_GC = re.compile(r'\bgc_delete_proxy_participant\([^,]+, (?P<guid>' + _GUID_TOKEN + r')\)')


def parse_lifecycle_trace(raw, *, domain_id):
    """Decode exact log statements, with byte offsets into a closed vendor file.

    Only the post-CAS L(:1c1 ...) trace on the same SPDP-known line witnesses
    renewal. A known participant, an advertisement or silence alone cannot.
    Expiry and GC deletion are distinct; neither implies a concurrency defect.
    """
    if not isinstance(raw, bytes) or type(domain_id) is not int or not 0 <= domain_id <= 231:
        raise ValueError('participant_lifecycle_trace_input_invalid')
    events, diagnostics, out_of_scope, offset = [], [], [], 0
    for number, line in enumerate(raw.splitlines(keepends=True), 1):
        header = _HEADER.match(line.decode('utf-8', errors='replace').rstrip('\r\n'))
        if header:
            body = header['body']
            spdp, expired, gc = _SPDP.search(body), _EXPIRED.search(body), _GC.search(body)
            match, kind = None, None
            if expired:
                match, kind = expired, 'expired'
            elif gc:
                match, kind = gc, 'gc_deleted'
            elif spdp:
                if re.search(r'\bNEW\b', spdp['detail']):
                    match, kind = spdp, 'discovered'
                elif '(known)' in spdp['detail'] and re.search(r' L\(:1c1 \d+\.\d+\)', spdp['detail']):
                    match, kind = spdp, 'refreshed'
            if match:
                guid = normalize_guid(match['guid'])
                if header['domain'] != str(domain_id):
                    diagnostics.append({'line': number, 'reason': 'lifecycle_domain_or_entity_mismatch'})
                elif not guid.endswith('000001c1'):
                    out_of_scope.append({'line': number, 'offset': offset, 'length': len(line),
                                         'entity_guid': guid, 'kind': 'nonparticipant_lease',
                                         'line_sha256': hashlib.sha256(line).hexdigest()})
                elif not line.endswith(b'\n'):
                    diagnostics.append({'line': number, 'reason': 'lifecycle_trace_truncated_line'})
                elif kind == 'expired' and int(match['deadline']) >= int(match['now']):
                    diagnostics.append({'line': number, 'reason': 'lifecycle_expiry_order_invalid'})
                else:
                    event = {'kind': kind, 'participant_guid': guid,
                             'unix_ns': int(Decimal(header['time']) * 10**9),
                             'thread': header['thread'].strip(), 'line': number,
                             'offset': offset, 'length': len(line),
                             'line_sha256': hashlib.sha256(line).hexdigest()}
                    if kind == 'expired':
                        event.update(expiry_deadline_elapsed_ns=int(match['deadline']),
                                     expiry_observed_elapsed_ns=int(match['now']))
                    events.append(event)
        offset += len(line)
    return {'schema': 'r2f.cyclone-participant-lifecycle/v1', 'events': events,
            'diagnostics': diagnostics, 'out_of_scope_events': out_of_scope, 'raw_sha256': hashlib.sha256(raw).hexdigest(),
            'source_references': SOURCE_REFERENCES, 'binary_source_equivalence': 'unknown',
            'candidate_eligible': False, 'race_status': 'not_established'}
