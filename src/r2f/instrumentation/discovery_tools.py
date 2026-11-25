"""Raw-backed binding/CLI attribution, auxiliary to the existing S1 oracle."""
from copy import deepcopy
import json
import re
from pathlib import Path

from .sedp_projection import _bound_json, _equal
from .sedp_wire import parse_sedp_capture


EXPECTED_SCHEMAS = {
    'DcpsParticipant': {'key', 'qos'},
    'DcpsTopic': {'key', 'topic_name', 'type_name', 'qos'},
    'DcpsEndpoint': {'key', 'participant_key', 'participant_instance_handle', 'topic_name', 'type_name', 'qos'},
}


def discovery_tools_schema(reference, capture, *, layers, bindings, field_policy=None):
    result = {'schema': 'r2f.discovery-tools-check/v1', 'status': 'unknown',
        'raw': deepcopy(reference.get('raw')), 'candidate_eligible': False,
        'responsibility_layer': 'observation_tools', 'review_required': True,
        'binding_obligations_closed': False, 'reasons': [], 'comparisons': [],
        'limitations': ['finite asynchronous collection; no absence or atomicity proof',
            'topic key is opaque and does not establish owner/generation',
            'normal control is not historical pre/post reproduction or a SUT finding',
            'Python source and installed binary equivalence is not established']}
    try:
        raw = _bound_json(reference['raw'])
        if (set(reference) != {'schema', 'status', 'raw'}
                or reference['schema'] != 'r2f.discovery-tools/v1'
                or raw.get('schema') != reference['schema'] or raw.get('status') != reference['status']
                or raw.get('candidate_eligible') is not False):
            raise ValueError('discovery_tools_reference_not_reproduced')
        result['producer_reasons'] = deepcopy(raw.get('reasons', []))
        events = raw['events']
        ready, = [event for event in events if event.get('event') == 'discovery_tools_ready']
        observation, = [event for event in events if event.get('event') == 'discovery_python_observation']
        started, = [event for event in events if event.get('event') == 'process_started']
        exited, = [event for event in events if event.get('event') == 'process_exit']
        manifest = _bound_json(raw['manifest'])
        if (ready.get('manifest') != raw['manifest'] or ready.get('version') != manifest.get('version')
                or ready.get('library') != manifest.get('library')
                or any(event.get('domain_id') != raw['domain_id'] or event.get('run_id') != raw['run_id']
                       or event.get('pid') != ready.get('pid') for event in events)
                or started.get('start_ticks') != ready.get('process_start_ticks')
                or any(observation.get(key) != ready.get(key)
                       for key in ('observer_generation', 'process_start_ticks'))):
            raise ValueError('discovery_tools_process_or_dependency_identity_mismatch')
        commands = [event for event in events if event.get('event') == 'worker_command_requested'
                    and event.get('command') == 'observe']
        done = [event for event in events if event.get('event') == 'command_done'
                and event.get('command_id') == observation.get('command_id')]
        if (len(commands) != 1 or len(done) != 1
                or commands[0].get('command_id') != observation.get('command_id')
                or not commands[0]['monotonic_ns'] <= observation['monotonic_ns'] <= done[0]['monotonic_ns']
                or not capture['ready']['unix_ns'] <= ready['time_ns'] <= observation['started_unix_ns']
                       <= observation['closed_unix_ns'] <= capture['unix_ns']):
            raise ValueError('discovery_tools_collection_chronology_unknown')
        if (raw.get('cleanup_complete') is not True or exited.get('returncode') != 0
                or exited.get('cleanup_confirmed') is not True
                or not raw['workers'] or any(worker.get('cleanup_confirmed') is not True
                    or worker.get('returncode') != 0 for worker in raw['workers'])):
            raise ValueError('discovery_tools_process_cleanup_unconfirmed')
        result.update(environment_manifest=deepcopy(raw['manifest']),
                      binding_version=ready['version'], library=deepcopy(ready['library']),
                      schemas=deepcopy(ready['schemas']), errors=deepcopy(observation.get('errors', [])),
                      observations=deepcopy(observation['samples']), control=deepcopy(ready['control']),
                      collection={'started_unix_ns': observation['started_unix_ns'],
                                  'closed_unix_ns': observation['closed_unix_ns'],
                                  'native_close_minus_python_close_ns': layers.get('timing', {}).get(
                                      'native_closed_unix_ns', observation['closed_unix_ns']) - observation['closed_unix_ns'],
                                  'basis': 'collection bounds, not per-sample receive timestamps'})
        if observation.get('errors') or observation.get('status') != 'observed':
            raise ValueError('binding_take_or_conversion_error')
        for name, fields in EXPECTED_SCHEMAS.items():
            if not fields <= set(ready['schemas'].get(name, [])):
                raise ValueError('binding_builtin_schema_mismatch:' + name)
        for label, rows in observation['samples'].items():
            name = {'DCPSParticipant': 'DcpsParticipant', 'DCPSTopic': 'DcpsTopic',
                    'DCPSPublication': 'DcpsEndpoint', 'DCPSSubscription': 'DcpsEndpoint'}[label]
            if any(row.get('sample_class') != name or row.get('fields') != ready['schemas'][name] for row in rows):
                raise ValueError('binding_sample_schema_mismatch:' + label)
        if layers.get('identity', {}).get('status') != 'established':
            raise ValueError('native_discovery_sources_not_qualified')
        wire = parse_sedp_capture(capture, field_policy=field_policy)
        rows = []
        for binding in bindings:
            guid, role = binding['endpoint_guid'], binding['role']
            observed = [sample for sample in observation['samples'][
                'DCPSPublication' if role.startswith('writer') else 'DCPSSubscription']
                if sample.get('key') == guid and sample.get('valid_data') is True and sample.get('instance_state') == 16]
            advertisements = [event for event in wire['events'] if event.get('endpoint_guid') == guid
                              and event.get('lifecycle') == 'alive']
            row = {'role': role, 'endpoint_guid': guid, 'status': 'unknown', 'fields': {},
                   'source': 'cyclonedds.builtin.BuiltinDataReader.take',
                   'wire_raw_refs': [deepcopy(event['raw_ref']) for event in advertisements]}
            if len(observed) == 1 and advertisements:
                sample, = observed
                qos = sample.get('qos', {})
                def enum(policy):
                    value = qos.get(policy, {}).get('kind')
                    return re.sub(r'(?<!^)(?=[A-Z])', '_', value).upper() if isinstance(value, str) else None
                projection = {'topic': sample['topic_name'], 'type': sample['type_name'],
                    'reliability': enum('Reliability'), 'durability': enum('Durability'),
                    'deadline': qos.get('Deadline', {}).get('deadline'),
                    'liveliness.kind': enum('Liveliness'),
                    'liveliness.lease_duration': qos.get('Liveliness', {}).get('lease_duration')}
                for name, python in projection.items():
                    values = {json.dumps(event['fields'][name].get('value'), sort_keys=True)
                              for event in advertisements if event['fields'].get(name, {}).get('status') == 'observed'}
                    if len(values) == 1 and python is not None:
                        value = json.loads(values.pop())
                        equal = python == value if name in ('topic', 'type') else _equal(name, value, python)
                        if equal is not None:
                            row['fields'][name] = {'python': python, 'wire': value, 'equal': equal}
                row['fields']['participant_guid'] = {'python': sample.get('participant_key'),
                    'wire_endpoint_prefix': guid[:24]+'000001c1',
                    'equal': sample.get('participant_key') == guid[:24]+'000001c1'}
                row['field_applicability'] = {'history': 'not_applicable_by_endpoint_schema',
                                              'depth': 'not_applicable_by_endpoint_schema'}
                if any(field.get('equal') is False for field in row['fields'].values()):
                    row['status'] = 'mismatch_requires_review'
                elif len(row['fields']) == 8:
                    row['status'] = 'consistent'
            rows.append(row)
        result['comparisons'] = rows
        participant_rows = []
        for guid in sorted({binding['endpoint_guid'][:24] + '000001c1' for binding in bindings}):
            python = [sample for sample in observation['samples']['DCPSParticipant']
                      if sample.get('key') == guid and sample.get('valid_data') is True
                      and sample.get('instance_state') == 16]
            native = [sample for sample in layers['participant']['builtin_observations']
                      if sample.get('guid') == guid and sample.get('valid_data') is True
                      and sample.get('instance_state') == 'ALIVE']
            advertisements = [event for event in layers['participant']['wire']['observations']
                              if event.get('participant_guid') == guid and event.get('status') == 'observed']
            participant_rows.append({'participant_guid': guid,
                'status': 'consistent' if len(python) == len(native) == 1 and advertisements else 'unknown',
                'basis': 'participant GUID and endpoint GUID prefix; not lifecycle generation',
                'python': deepcopy(python), 'native': deepcopy(native),
                'wire_raw_refs': [deepcopy(event['raw_ref']) for event in advertisements]})
        result['participant_comparisons'] = participant_rows
        if not participant_rows or any(row['status'] != 'consistent' for row in participant_rows):
            result['reasons'].append('binding_participant_identity_incomplete_or_different')
        control = ready['control']
        topic_matches = [row for row in layers['topic'].get('field_comparisons', [])
                         if row.get('topic') == control['topic'] and row.get('type') == control['type']]
        python_control = [sample for sample in observation['samples']['DCPSTopic']
                          if sample.get('topic_name') == control['topic'] and sample.get('type_name') == control['type']
                          and sample.get('valid_data') is True and sample.get('type_id_status') == 'available']
        topic_comparisons = []
        for native in layers['topic'].get('field_comparisons', []):
            matching = [sample for sample in observation['samples']['DCPSTopic']
                        if sample.get('topic_name') == native.get('topic')
                        and sample.get('type_name') == native.get('type')
                        and sample.get('valid_data') is True and sample.get('instance_state') == 16
                        and sample.get('type_id_status') == 'available']
            if not matching:
                topic_comparisons.append({'topic': native.get('topic'), 'type': native.get('type'),
                    'status': 'unknown', 'fields': {}, 'missing': ['python_topic_observation']})
            for sample in matching:
                fields, missing = {}, []
                history = sample.get('history', {})
                for name in ('history', 'depth'):
                    observed = native.get('fields', {}).get(name, {})
                    if (name == 'depth' and history.get('kind') == 'KEEP_ALL'
                            and native.get('fields', {}).get('history', {}).get('builtin') == 'KEEP_ALL'
                            and native.get('fields', {}).get('history', {}).get('wire') == 'KEEP_ALL'):
                        fields[name] = {'status': 'not_applicable', 'reason': 'ignored_for_KEEP_ALL'}
                        continue
                    values = {'python': history.get('kind' if name == 'history' else name),
                              'builtin': observed.get('builtin'), 'wire': observed.get('wire')}
                    if history.get('status') != 'observed' or None in values.values():
                        missing.append(name)
                    else:
                        fields[name] = dict(values, equal=len(set(values.values())) == 1)
                status = ('mismatch_requires_review' if any(field.get('equal') is False for field in fields.values())
                          else 'unknown' if missing or native.get('status') != 'consistent' else 'consistent')
                topic_comparisons.append({'scope': 'topic_type_field_projection_not_instance_identity',
                    'topic': native.get('topic'), 'type': native.get('type'), 'status': status,
                    'fields': fields, 'missing': missing})
        result['topic_comparisons'] = topic_comparisons
        control_comparisons = [row for row in topic_comparisons
                               if row['topic'] == control['topic'] and row['type'] == control['type']]
        control_status = ('mismatch_requires_review' if any(row['status'] == 'mismatch_requires_review'
                          for row in control_comparisons) else 'consistent' if control_comparisons
                          and all(row['status'] == 'consistent' for row in control_comparisons) else 'unknown')
        result['positive_control'] = {'status': control_status,
            'native_wire': deepcopy(topic_matches), 'python': deepcopy(python_control),
            'field_comparisons': control_comparisons,
            'candidate_eligible': False, 'sut_observation': False}
        if not rows or any(row['status'] != 'consistent' for row in rows):
            result['reasons'].append('binding_endpoint_fields_incomplete_or_different')
        if result['positive_control']['status'] != 'consistent':
            result['reasons'].append('full_xtypes_topic_control_not_observed')
        if not topic_comparisons or any(row['status'] != 'consistent' for row in topic_comparisons):
            result['reasons'].append('binding_topic_fields_incomplete_or_different')
        if raw['status'] != 'observed':
            result['reasons'].extend(raw.get('reasons', []) or ['tools_producer_unknown'])
        result['binding_obligations_closed'] = not result['reasons']
        cli = [event for event in events if event.get('event') == 'discovery_cli_observation']
        selected = [sample for label in ('DCPSPublication', 'DCPSSubscription')
                    for sample in observation['samples'][label]
                    if sample.get('key') in {binding['endpoint_guid'] for binding in bindings}
                    and sample.get('valid_data') is True and sample.get('instance_state') == 16]
        result['cli_comparisons'] = [compare_cli_projection(event, selected) for event in cli]
        if (len(cli) != 2 or {event.get('subcommand') for event in cli} != {'ls', 'ps'}
                or any(event.get('entrypoint') != 'cyclonedds.tools.cli.main:cli'
                       or not capture['ready']['unix_ns'] <= event['started_unix_ns']
                           <= event['closed_unix_ns'] <= capture['unix_ns'] for event in cli)
                or any(row['status'] != 'consistent' for row in result['cli_comparisons'])):
            result['reasons'].append('cli_projection_incomplete_or_different')
        if not result['reasons']:
            result.update(status='consistent', review_required=False)
    except (AttributeError, KeyError, IndexError, OSError, TypeError, ValueError) as exc:
        result['reasons'].append(str(exc))
    coverage = Path(__file__).parents[1] / 'modules' / 's1' / 's1_coverage.json'
    metadata = json.loads(coverage.read_text())
    # Immutable knowledge only; no fixture, control or later pass reproduces a case.
    result['historical_knowledge'] = [
        {key: deepcopy(case[key]) for key in ('case_id', 'source_url', 'research_classification',
                                              'fix', 'versions', 'reproduced_here', 'new_discovery')}
        for case in metadata['cases'] if case['case_id'] in ('py130', 'py200')]
    return result


def compare_cli_projection(receipt, endpoints):
    """Version-qualified CLI text visibility; never synthesize unavailable fields."""
    import re
    result = {'subcommand': receipt.get('subcommand'), 'status': 'unknown',
              'candidate_eligible': False, 'responsibility_layer': 'unresolved',
              'sut_violation_established': False,
              'assurance': 'textual_field_visibility_not_atomic_row_or_instance_identity',
              'field_applicability': {'participant': 'displayed_uuid', 'topic': 'displayed_text',
                  'endpoint': 'displayed_uuid' if receipt.get('subcommand') == 'ls' else 'not_presented_by_ps',
                  'type': 'displayed_text' if receipt.get('subcommand') == 'ls' else 'not_presented_by_ps',
                  'publisher_gid_per_sample': 'unavailable_api', 'generation': 'unavailable_api'},
              'missing': []}
    if (receipt.get('exit_code') != 0 or receipt.get('thread_errors') or receipt.get('error')
            or 'Traceback (most recent call last)' in receipt.get('stderr', '')
            or 'Traceback (most recent call last)' in receipt.get('stdout', '')):
        result.update(status='tool_error', responsibility_layer='observation_tool')
        return result
    if receipt.get('subcommand') not in ('ls', 'ps') or not endpoints:
        result['missing'].append('supported_command_and_independent_endpoints_required')
        return result
    text = re.sub(r'\x1b\[[0-9;]*[A-Za-z]', '', receipt.get('stdout', ''))
    guids = {value.replace('-', '').lower() for value in re.findall(
        r'(?<![0-9a-fA-F])[0-9a-fA-F]{8}(?:-[0-9a-fA-F]{4}){3}-[0-9a-fA-F]{12}(?![0-9a-fA-F])', text)}
    for endpoint in endpoints:
        needed = {'participant': endpoint['participant_key'], 'topic': endpoint['topic_name']}
        if receipt['subcommand'] == 'ls':
            needed.update(endpoint=endpoint['key'], type=endpoint['type_name'])
        for name, value in needed.items():
            present = value in guids if name in ('participant', 'endpoint') else re.search(
                r'(?<![\w/:])' + re.escape(value) + r'(?![\w/:])', text) is not None
            if not present:
                result['missing'].append({'endpoint_guid': endpoint['key'], 'field': name, 'expected_observed': value})
    if not result['missing']:
        result.update(status='consistent', responsibility_layer='observation_tool')
    else:
        result.update(status='projection_gap_requires_review', responsibility_layer='observation_tool_or_observation_gap')
    return result
