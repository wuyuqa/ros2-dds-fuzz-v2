"""Hash-bound auxiliary discovery layers; never an oracle or candidate source."""
from copy import deepcopy
import hashlib
import re
from pathlib import Path

from .sedp_projection import _bound_json
from .spdp_wire import parse_spdp_capture
from .topic_wire import parse_topic_capture


def discovery_layers_schema(reference, capture, *, field_policy=None):
    result = {'schema': 'r2f.discovery-layers/v1',
              'identity': {'status': 'unknown', 'reasons': []},
              'responsibility_layer': 'harness', 'candidate_eligible': False,
              'all_layer_obligations_closed': False,
              'participant': {'builtin_observations': [], 'api_lease_duration': 'unavailable_api',
                              'process_generation': 'unknown'},
              'topic': {'builtin_observations': [], 'key_kind': 'opaque16_not_guid',
                        'owner_generation': 'unknown'},
              'limitations': ['native participant API has no participant discovery lease',
                  'participant GUID/prefix is not process generation',
                  'topic opaque16 key does not establish owner or generation',
                  'capture may include observer activity; observer GUID is separately identified',
                  'take wall time is not a per-sample wire reception timestamp',
                  'binding and CLI observations remain outside this checkpoint']}
    try:
        if set(reference) != {'schema', 'status', 'raw'}:
            raise ValueError('builtin_reference_keys_mismatch')
        raw = _bound_json(reference['raw'])
        if not isinstance(raw, dict):
            raise ValueError('builtin_raw_object_required')
        if (reference['schema'] != 'r2f.discovery-builtins/v1'
                or raw['schema'] != reference['schema'] or raw['status'] != reference['status']):
            raise ValueError('builtin_reference_projection_mismatch')
        result['native_diagnostics'] = {'raw': deepcopy(reference['raw']),
            'status': raw.get('status'), 'reasons': deepcopy(raw.get('reasons', [])),
            'receipts': deepcopy(raw.get('receipts', []))}
        if (type(raw.get('domain_id')) is not int
                or raw.get('admission', {}).get('domain_id') != raw['domain_id']):
            raise ValueError('builtin_admission_domain_mismatch')
        receipt = _bound_json(capture['receipt_raw'])
        if receipt != {k: v for k, v in capture.items() if k != 'receipt_raw'}:
            raise ValueError('discovery_capture_receipt_not_reproduced')
        ready = capture['ready']['unix_ns']
        stopped = capture['unix_ns']
        start, end = raw['started_unix_ns'], raw['closed_unix_ns']
        if (any(type(t) is not int for t in (ready, stopped, start, end))
                or not ready <= start <= end <= stopped):
            raise ValueError('builtin_capture_chronology_mismatch')
        if Path(reference['raw']['path']).resolve() == Path(capture['raw']['path']).resolve():
            raise ValueError('builtin_wire_sources_not_independent')
        profile = raw['profile']
        profile_data = Path(profile['path']).read_bytes()
        if (hashlib.sha256(profile_data).hexdigest() != profile['sha256']
                or raw['environment'] != {'CYCLONEDDS_URI': profile['path']}
                or b'<Discovery><EnableTopicDiscoveryEndpoints>true</EnableTopicDiscoveryEndpoints>' not in profile_data):
            raise ValueError('builtin_profile_identity_mismatch')
        samples = {'DCPSParticipant': [], 'DCPSTopic': []}
        last_time = start
        last_mono = raw['started_monotonic_ns']
        close_mono = raw['closed_monotonic_ns']
        closed = False
        opened = False
        readers = {}
        sample_refs = {topic: [] for topic in samples}
        for receipt_index, row in enumerate(raw['receipts']):
            if type(row['unix_ns']) is not int or not last_time <= row['unix_ns'] <= end:
                raise ValueError('builtin_receipt_chronology_mismatch')
            if (type(row['monotonic_ns']) is not int or type(last_mono) is not int
                    or type(close_mono) is not int or not last_mono <= row['monotonic_ns'] <= close_mono):
                raise ValueError('builtin_receipt_monotonic_chronology_mismatch')
            last_time = row['unix_ns']
            last_mono = row['monotonic_ns']
            value = row['receipt']
            if closed:
                raise ValueError('builtin_receipt_after_close')
            if row['operation'] == 'open':
                if (value.get('receipt_kind') != 'native_participant_created'
                        or value.get('real_execution') is not True
                        or value.get('accepted', True) is not True or value.get('ok', True) is not True
                        or type(value.get('participant_handle')) is not int or value['participant_handle'] <= 0
                        or not isinstance(value.get('guid'), str)
                        or re.fullmatch('[0-9a-f]{32}', value['guid']) is None):
                    raise ValueError('builtin_open_not_accepted')
                if value.get('domain_id') != raw['domain_id']:
                    raise ValueError('builtin_open_domain_mismatch')
                if opened or value.get('guid') != raw.get('observer_guid'):
                    raise ValueError('builtin_open_identity_mismatch')
                opened = True
            if row['operation'].startswith('reader:'):
                topic = row['operation'].split(':', 1)[1]
                if (not opened or topic not in samples or topic in readers or value.get('topic') != topic
                        or value.get('receipt_kind') != 'native_builtin_reader_created'
                        or value.get('real_execution') is not True
                        or value.get('topic_handle') != (0x7fff0001 if topic == 'DCPSParticipant' else 0x7fff0002)
                        or value.get('accepted', True) is not True or value.get('ok', True) is not True
                        or type(value.get('reader_handle')) is not int or value['reader_handle'] <= 0
                        or value['reader_handle'] in readers.values()):
                    raise ValueError('builtin_reader_identity_mismatch')
                readers[topic] = value.get('reader_handle')
            if row['operation'].startswith('take:'):
                topic = row['operation'].split(':', 1)[1]
                if topic not in readers:
                    raise ValueError('builtin_take_before_reader')
                if (type(value.get('reader_handle')) is not int
                        or value.get('reader_handle') != readers[topic]
                        or value.get('receipt_kind') != ('native_builtin_participants'
                            if topic == 'DCPSParticipant' else 'native_builtin_topics')):
                    raise ValueError('builtin_take_reader_identity_mismatch')
                if (value.get('accepted') is not True or value.get('ok') is not True or value.get('loan_cleanup_complete') is not True
                        or (value.get('take_return_code', 0) > 0 and value.get('loan_returned') is not True)):
                    raise ValueError('builtin_take_or_loan_unknown')
                if type(value.get('take_return_code')) is not int or value['take_return_code'] != len(value['samples']):
                    raise ValueError('builtin_take_sample_count_mismatch')
                samples[topic].extend(deepcopy(value['samples']))
                sample_refs[topic].extend({**reference['raw'], 'receipt_index': receipt_index,
                    'sample_index': i} for i in range(len(value['samples'])))
            if row['operation'] == 'close':
                closed = (value.get('cleanup_complete') is True
                          and value.get('receipt_kind') == 'native_participant_closed'
                          and value.get('real_execution') is True)
        if not opened:
            raise ValueError('builtin_open_receipt_missing')
        if samples != raw['samples']:
            raise ValueError('builtin_samples_not_reproduced_from_receipts')
        if not closed or raw['cleanup_complete'] is not True:
            raise ValueError('builtin_cleanup_unknown')
        spdp = parse_spdp_capture(capture, field_policy=field_policy)
        topic_wire = parse_topic_capture(capture, field_policy=field_policy)
        for name, parsed in (('participant', spdp), ('topic', topic_wire)):
            if parsed.get('raw') != capture['raw']:
                # The parser enriches references with capture format metadata.
                if (not parsed.get('raw') or any(parsed['raw'].get(k) != capture['raw'].get(k)
                        for k in ('path', 'sha256'))):
                    raise ValueError('discovery_wire_identity_unknown:' + name + ':'
                                     + ','.join(parsed.get('reasons', [])))
        result['identity'] = {'status': 'established', 'reasons': []}
        result.update(status=raw['status'], reasons=deepcopy(raw['reasons']),
                      independent_sources={'builtin': deepcopy(reference['raw']),
                                           'wire': deepcopy(capture['raw'])},
                      observer_guid=raw.get('observer_guid'),
                      native_receipts=deepcopy(raw['receipts']),
                      profile=deepcopy(profile), environment=deepcopy(raw['environment']),
                      timing={'native_started_unix_ns': start, 'native_closed_unix_ns': end,
                              'capture_ready_unix_ns': ready, 'capture_stopped_unix_ns': stopped,
                              'basis': 'capture and local take wall times; asynchronous observations'})
        result['participant'].update(builtin_observations=samples['DCPSParticipant'], wire=spdp,
            builtin_raw_refs=sample_refs['DCPSParticipant'],
            applicability={'participant_guid': {'builtin': 'sample.guid', 'wire': 'PID_PARTICIPANT_GUID'},
                           'lease_duration': {'builtin': 'unavailable_api', 'wire': 'PID_PARTICIPANT_LEASE_DURATION'}},
            sut_process_association='unknown')
        result['topic'].update(builtin_observations=samples['DCPSTopic'], wire=topic_wire,
            builtin_raw_refs=sample_refs['DCPSTopic'],
            applicability={'topic': {'builtin': 'sample.topic_name', 'wire': 'PID_TOPIC_NAME'},
                           'type': {'builtin': 'sample.type_name', 'wire': 'PID_TYPE_NAME'},
                           'history': {'builtin': 'sample.qos:dds_qget_history', 'wire': 'PID_HISTORY'},
                           'depth': {'builtin': 'sample.qos:dds_qget_history; not_applicable_keep_all',
                                     'wire': 'PID_HISTORY; not_applicable_keep_all'}})
        timing_reasons = []
        if raw.get('timestamp_clock') != 'unix_ns_realtime':
            timing_reasons.append('native_realtime_clock_not_established')
        if capture['ready'].get('timestamp_clock') != 'kernel_SO_TIMESTAMPNS_realtime':
            timing_reasons.append('capture_realtime_clock_not_established')
        mono_start, mono_end = raw.get('started_monotonic_ns'), raw.get('closed_monotonic_ns')
        if (type(mono_start) is not int or type(mono_end) is not int or mono_start > mono_end
                or abs((end-start) - (mono_end-mono_start)) > 50_000_000):
            timing_reasons.append('native_clock_continuity_unknown')
        result['timing'].update(status='unknown' if timing_reasons else 'qualified',
                                reasons=timing_reasons)
        # Receipt qualification and instance reconciliation are separate. Exact
        # participant GUIDs can correlate advertisements only, never a process
        # generation. Topic names/types never establish topic instance identity.
        correlations, association_reasons = [], []
        for i, sample in enumerate(samples['DCPSParticipant']):
            guid = sample.get('guid')
            matches = [e for e in spdp['observations'] if e.get('participant_guid') == guid]
            duplicates = sum(s.get('guid') == guid for s in samples['DCPSParticipant'])
            reasons = []
            if not matches:
                reasons.append('participant_guid_unmatched')
            if duplicates != 1:
                reasons.append('participant_builtin_identity_ambiguous')
            if guid == raw.get('observer_guid'):
                reasons.append('observer_self_not_sut')
            if sample.get('valid_data') is not True or sample.get('instance_state') != 'ALIVE':
                reasons.append('participant_sample_not_alive_valid_data')
            if any(e.get('status') != 'observed' for e in matches):
                reasons.append('participant_wire_observation_unknown')
            association_reasons.extend(reasons)
            correlations.append({'participant_guid': guid,
                'status': 'unknown' if reasons else 'guid_only', 'reasons': reasons,
                'builtin_raw_ref': sample_refs['DCPSParticipant'][i],
                'wire_raw_refs': [deepcopy(e['raw_ref']) for e in matches]})
        result['participant']['association'] = {'status': 'unknown',
            'reasons': sorted(set(association_reasons)) or ['process_lifecycle_association_unavailable'],
            'guid_correlations': correlations}
        result['topic']['association'] = {'status': 'unknown',
            'reasons': ['topic_instance_owner_generation_unavailable', 'opaque16_key_not_wire_instance_guid']}
        for layer in ('participant', 'topic'):
            result[layer]['reconciliation_status'] = 'unknown'
            result[layer]['observation_status'] = (
                'observed' if raw['status'] == 'observed' and result[layer]['builtin_observations']
                and result[layer]['wire']['status'] == 'observed' else 'unknown')
            if result[layer]['observation_status'] != 'observed':
                result['status'] = 'unknown'
                result['reasons'].extend(layer + ':' + reason for reason in result[layer]['wire']['reasons'])
            for event in result[layer]['wire']['observations']:
                event['native_close_minus_wire_ns'] = None if timing_reasons else end - event['unix_ns']
                event['timing_basis'] = 'local collection close minus wire; not sample correlation'
    except (AttributeError, KeyError, OSError, TypeError, ValueError) as exc:
        result['identity'] = {'status': 'unknown', 'reasons': [str(exc)]}
        for name in ('participant', 'topic'):
            result[name]['builtin_observations'] = []
        result['status'] = 'unknown'
    for name in ('participant', 'topic'):
        result[name].setdefault('association', {'status': 'unknown', 'reasons': ['sources_not_qualified']})
        result[name].setdefault('reconciliation_status', 'unknown')
    return result
