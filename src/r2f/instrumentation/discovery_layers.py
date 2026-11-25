"""Hash-bound auxiliary discovery layers; never an oracle or candidate source."""
from copy import deepcopy
import hashlib
import re
from pathlib import Path

from .sedp_projection import _bound_json
from .spdp_wire import parse_spdp_capture
from .topic_wire import parse_topic_capture


def discovery_layers_schema(reference, capture, *, field_policy=None, endpoint_bindings=()):
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
        result['topic']['field_comparisons'] = _topic_field_comparisons(
            samples['DCPSTopic'], sample_refs['DCPSTopic'], topic_wire)
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
        result['participant']['process_correlations'] = _process_correlations(
            endpoint_bindings, samples['DCPSParticipant'], sample_refs['DCPSParticipant'],
            spdp, raw['domain_id'])
        result['topic']['association'] = {'status': 'unknown',
            'reasons': ['topic_instance_owner_generation_unavailable', 'opaque16_key_not_wire_instance_guid']}
        native_collection_qualified = not (set(raw['reasons']) - {'builtin_samples_unavailable'})
        for layer in ('participant', 'topic'):
            result[layer]['reconciliation_status'] = 'unknown'
            result[layer]['observation_status'] = (
                'observed' if native_collection_qualified and result[layer]['builtin_observations']
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


def _topic_field_comparisons(samples, refs, wire):
    """Compare only stable advertised fields; name/type is not an instance ID.

    Asynchronous reads cannot prove a discrepancy is in the core or binding.
    Keep all conflicting advertisements and send a mismatch for review only.
    """
    rows = []
    for sample, ref in zip(samples, refs):
        matches = [event for event in wire['observations']
                   if event.get('status') == 'observed'
                   and event['fields']['topic'].get('value') == sample.get('topic_name')
                   and event['fields']['type'].get('value') == sample.get('type_name')]
        history = sample.get('history', {})
        row = {'scope': 'topic_type_field_projection_not_instance_identity',
               'topic': sample.get('topic_name'), 'type': sample.get('type_name'),
               'builtin_raw_ref': deepcopy(ref),
               'wire_raw_refs': [deepcopy(event['raw_ref']) for event in matches],
               'fields': {}, 'status': 'unknown', 'reasons': [],
               'candidate_eligible': False, 'responsibility_layer': 'unresolved'}
        if not matches:
            row['reasons'].append('no_same_name_type_wire_advertisement')
        if (sample.get('valid_data') is not True or sample.get('instance_state') != 'ALIVE'
                or history.get('status') != 'observed' or wire.get('status') != 'observed'):
            row['reasons'].append('topic_source_or_sample_not_qualified')
        for name in ('history', 'depth'):
            builtin = history.get('kind' if name == 'history' else name)
            values = {event['fields'][name].get('value') for event in matches}
            # KEEP_ALL's depth has no semantic comparison, even if C exposes it.
            if name == 'depth' and history.get('kind') == 'KEEP_ALL':
                row['fields'][name] = {'status': 'not_applicable', 'reason': 'ignored_for_KEEP_ALL'}
                continue
            if builtin is None or len(values) != 1 or None in values:
                row['reasons'].append('missing_or_ambiguous_topic_field:' + name)
                continue
            value, = values
            row['fields'][name] = {'builtin': builtin, 'wire': value, 'equal': builtin == value}
        if not row['reasons']:
            row['status'] = ('mismatch_requires_review' if any(
                field.get('equal') is False for field in row['fields'].values()) else 'consistent')
        rows.append(row)
    return rows


def _process_correlations(bindings, samples, refs, wire, domain):
    """Join an observed local parent to raw process/endpoint and SPDP identity.

    An endpoint generation is never renamed a participant lifecycle generation.
    Each link is a bounded observation; no atomicity/continuous-presence claim.
    """
    from .sedp_projection import _binding_times, _lifecycle_event
    rows = []
    for binding in bindings:
        row = {key: deepcopy(binding.get(key)) for key in
               ('role', 'generation', 'pid', 'process_start_ticks', 'endpoint_guid')}
        row.update(status='unknown', reasons=[], candidate_eligible=False,
                   participant_lifecycle_generation='unknown',
                   lifecycle_raw_refs=[deepcopy(binding.get(key)) for key in ('begin_raw_ref', 'return_raw_ref')],
                   wire_raw_refs=[], builtin_raw_refs=[])
        try:
            returned = _lifecycle_event(binding['return_raw_ref'])
            times = _binding_times(binding, binding['endpoint_guid'], '')
            if times is None:
                raise ValueError('endpoint_process_lifecycle_not_bound')
            ownership = returned.get('participant_ownership', {})
            guid = ownership.get('participant_gid')
            root = ownership.get('participant_root_observation', {})
            identities = ownership.get('entity_identities', [])
            if (ownership.get('schema') != 'r2f.cyclone-participant-ownership/v2'
                    or ownership.get('complete') is not True or ownership.get('status') != 'observed'
                    or ownership.get('stable_sample_count') != 2 or ownership.get('candidate_eligible') is not False
                    or ownership.get('domain_id') != domain
                    or ownership.get('local_gid') != binding['endpoint_guid']
                    or ownership.get('role_kind') != re.sub(r'[2-4]$', '', binding['role'])
                    or not isinstance(guid, str) or re.fullmatch('[0-9a-f]{24}000001c1', guid) is None
                    or ownership.get('parent_chain', [None])[0] != ownership.get('local_handle')
                    or ownership.get('parent_chain', [None])[-1] != ownership.get('participant_handle')
                    or root.get('participant_handle') != ownership.get('participant_handle')
                    or root.get('participant_self_return') != ownership.get('participant_handle')
                    or ownership.get('participant_handle') not in root.get('lookup_participants', [])
                    or not ownership.get('provenance')
                    or not times['begin_monotonic_ns'] <= ownership['started_monotonic_ns']
                        <= ownership['ended_monotonic_ns'] <= times['return_monotonic_ns']
                    or len([i for i in identities if i.get('handle') == ownership['local_handle']
                            and i.get('gid') == binding['endpoint_guid']]) != 1
                    or len([i for i in identities if i.get('handle') == ownership['participant_handle']
                            and i.get('gid') == guid]) != 1):
                raise ValueError('native_ownership_not_bound')
            row['participant_guid'] = guid
            row['ownership'] = deepcopy(ownership)
            matches = [event for event in wire['observations']
                       if event.get('participant_guid') == guid and event.get('status') == 'observed']
            row['wire_raw_refs'] = [deepcopy(event['raw_ref']) for event in matches]
            row['builtin_raw_refs'] = [deepcopy(ref) for sample, ref in zip(samples, refs)
                if sample.get('guid') == guid and sample.get('valid_data') is True
                and sample.get('instance_state') == 'ALIVE']
            if not matches or not row['builtin_raw_refs']:
                raise ValueError('independent_participant_observation_unavailable')
            row['status'] = 'endpoint_process_correlated'
        except (KeyError, IndexError, OSError, TypeError, ValueError) as exc:
            row['reasons'].append(str(exc))
        rows.append(row)
    return rows
