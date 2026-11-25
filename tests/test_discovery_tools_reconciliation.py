"""Raw-backed auxiliary observations; literal offline data is never live evidence."""
from copy import deepcopy
import json
from pathlib import Path
import uuid

from test_discovery_layers_projection import parts_with_layers
from test_instrumentation_sedp_projection import BASE, GUID, PREFIX, hashed, jsonraw
from r2f.instrumentation.discovery_tools import EXPECTED_SCHEMAS
from r2f.instrumentation.sedp import parse_sedp_capture, reconcile_discovery_projection


def tools_fixture(root):
    parts = list(parts_with_layers(root))
    capture = parts[0]['capture_receipt']
    capture['raw'] = hashed(Path(capture['raw']['path']),
        Path(capture['raw']['path']).read_bytes().replace(
            bytes.fromhex('0102030405060708090a0b0c'), bytes.fromhex(PREFIX)))
    capture['receipt_raw'] = jsonraw(root/'capture.json',
        {key: value for key, value in capture.items() if key != 'receipt_raw'})
    parts[0] = parse_sedp_capture(capture)
    native_path = Path(parts[-1]['raw']['path'])
    native = json.loads(native_path.read_text().replace('01'*12+'000001c1', PREFIX+'000001c1'))
    parts[-1]['raw'] = jsonraw(native_path, native)
    schemas = {name: sorted(fields) for name, fields in EXPECTED_SCHEMAS.items()}
    participant = PREFIX+'000001c1'
    def sample(name, key, **extra):
        return {'sample_class': name, 'fields': schemas[name], 'key': key,
                'valid_data': True, 'instance_state': 16, **extra}
    samples = {
        'DCPSParticipant': [sample('DcpsParticipant', participant)],
        'DCPSTopic': [sample('DcpsTopic', 'ab'*16, topic_name='demo', type_name='Msg',
            type_id_status='available', history={'status': 'observed', 'kind': 'KEEP_LAST', 'depth': 7})],
        'DCPSPublication': [sample('DcpsEndpoint', GUID, participant_key=participant,
            topic_name='rt/test', type_name='std_msgs::msg::dds_::String_',
            qos={'Reliability': {'kind': 'Reliable'}, 'Durability': {'kind': 'Volatile'},
                 'Deadline': {'deadline': 2**63-1},
                 'Liveliness': {'kind': 'Automatic', 'lease_duration': 2**63-1}})],
        'DCPSSubscription': [],
    }
    library = {'path': '/offline/libddsc.so', 'sha256': 'a'*64}
    manifest = jsonraw(root/'manifest.json', {'version': 'offline', 'library': library})
    common = {'pid': 88, 'domain_id': 19, 'run_id': 'offline'}
    def event(name, **values):
        return dict(common, event=name, **values)
    ready = event('discovery_tools_ready', manifest=manifest, version='offline', library=library,
        process_start_ticks=123, observer_generation='offline-generation', schemas=schemas,
        time_ns=BASE+101, control={'topic': 'demo', 'type': 'Msg'})
    observation = event('discovery_python_observation', command_id=1, monotonic_ns=20,
        process_start_ticks=123, observer_generation='offline-generation', samples=samples,
        started_unix_ns=BASE+135, closed_unix_ns=BASE+145, status='observed', errors=[])
    text = f'{uuid.UUID(participant)} {uuid.UUID(GUID)} rt/test std_msgs::msg::dds_::String_'
    events = [event('process_started', start_ticks=123), ready,
        event('worker_command_requested', command='observe', command_id=1, monotonic_ns=10),
        observation, event('command_done', command_id=1, monotonic_ns=30),
        *[event('discovery_cli_observation', subcommand=command, stdout=text, stderr='',
                exit_code=0, thread_errors=[], entrypoint='cyclonedds.tools.cli.main:cli',
                started_unix_ns=BASE+145, closed_unix_ns=BASE+150) for command in ('ls', 'ps')],
        event('process_exit', returncode=0, cleanup_confirmed=True)]
    raw = {'schema': 'r2f.discovery-tools/v1', 'status': 'observed', 'candidate_eligible': False,
           'manifest': manifest, 'domain_id': 19, 'run_id': 'offline', 'events': events,
           'cleanup_complete': True, 'workers': [{'returncode': 0, 'cleanup_confirmed': True}]}
    return parts, raw


def reconcile(root, parts, raw, **extra):
    reference = {'schema': raw['schema'], 'status': raw['status'], 'raw': jsonraw(root/'tools.json', raw)}
    return reconcile_discovery_projection(parts[0], parts[1], endpoint_bindings=parts[2],
        observation_window=parts[3], discovery_layers=parts[4], discovery_tools=reference,
        **extra)['schema_check']['discovery_tools']


def test_missing_participant_binding_keeps_observation_obligation_open(tmp_path):
    parts, raw = tools_fixture(tmp_path)
    assert reconcile(tmp_path, parts, raw)['status'] == 'consistent'
    observation = next(event for event in raw['events'] if event['event'] == 'discovery_python_observation')
    observation['samples']['DCPSParticipant'] = []
    result = reconcile(tmp_path, parts, raw)
    assert result['binding_obligations_closed'] is False
    assert result['review_required'] is True
    assert 'binding_participant_identity_incomplete_or_different' in result['reasons']


def test_binding_topic_history_is_compared_to_independent_native_and_wire(tmp_path):
    parts, raw = tools_fixture(tmp_path)
    observation = next(event for event in raw['events'] if event['event'] == 'discovery_python_observation')
    observation['samples']['DCPSTopic'][0]['history']['depth'] = 999
    result = reconcile(tmp_path, parts, raw)
    assert result['positive_control']['status'] == 'mismatch_requires_review'
    assert result['binding_obligations_closed'] is False
    comparison, = result['positive_control']['field_comparisons']
    assert comparison['fields']['depth'] == {'python': 999, 'builtin': 7, 'wire': 7, 'equal': False}


def test_auxiliary_comparison_covers_additional_role_without_narrowing_primary_pair(tmp_path):
    parts, raw = tools_fixture(tmp_path)
    extra = deepcopy(parts[2][0])
    extra.update(role='writer2', endpoint_guid=PREFIX+'00002003')
    result = reconcile(tmp_path, parts, raw, discovery_endpoint_bindings=[*parts[2], extra])
    assert [row['role'] for row in result['comparisons']] == ['writer', 'writer2']
    assert result['comparisons'][1]['status'] == 'unknown'
    assert result['binding_obligations_closed'] is False


def test_obligation_summary_retains_missing_sut_topic_despite_a_healthy_control(tmp_path):
    parts, raw = tools_fixture(tmp_path)
    ref = {'schema': raw['schema'], 'status': raw['status'], 'raw': jsonraw(tmp_path/'tools.json', raw)}
    result = reconcile_discovery_projection(parts[0], parts[1], endpoint_bindings=parts[2],
        observation_window=parts[3], discovery_layers=parts[4], discovery_tools=ref)
    schema = result['schema_check']
    summary = schema['observation_obligations']
    assert schema['discovery_tools']['positive_control']['status'] == 'consistent'
    assert summary['all_closed'] is False
    obligations = {row['id']: row for row in summary['items']}
    assert obligations['topic_for_selected_endpoints']['status'] == 'unknown'
    assert obligations['topic_for_selected_endpoints']['missing'] == [GUID]
    assert 'topic_owner_generation' in summary['api_limits']
    assert schema['all_layer_obligations_closed'] is False


def test_binding_endpoint_qos_difference_is_retained_for_review(tmp_path):
    parts, raw = tools_fixture(tmp_path)
    observation = next(event for event in raw['events'] if event['event'] == 'discovery_python_observation')
    observation['samples']['DCPSPublication'][0]['qos']['Reliability']['kind'] = 'BestEffort'
    result = reconcile(tmp_path, parts, raw)
    assert result['comparisons'][0]['status'] == 'mismatch_requires_review'
    assert result['comparisons'][0]['fields']['reliability']['equal'] is False
    assert result['binding_obligations_closed'] is False


def test_complete_raw_backed_observations_can_close_and_missing_fields_reopen(tmp_path):
    from test_instrumentation_sedp_projection import cdr, param, pcap
    from test_topic_wire import HISTORY, SENTINEL, frame, packet
    parts, raw = tools_fixture(tmp_path)
    capture = parts[0]['capture_receipt']
    payload = param(5, cdr('rt/test')) + param(7, cdr('std_msgs::msg::dds_::String_')) + HISTORY + SENTINEL
    capture['raw'] = hashed(Path(capture['raw']['path']), Path(capture['raw']['path']).read_bytes()
                            + pcap([(BASE+127, frame(packet(payload).replace(
                                bytes.fromhex('0102030405060708090a0b0c'), bytes.fromhex(PREFIX))))])[24:])
    capture['ready']['timestamp_clock'] = 'kernel_SO_TIMESTAMPNS_realtime'
    capture['receipt_raw'] = jsonraw(tmp_path/'capture.json',
        {key: value for key, value in capture.items() if key != 'receipt_raw'})
    parts[0] = parse_sedp_capture(capture)
    native_path = Path(parts[-1]['raw']['path'])
    native = json.loads(native_path.read_text())
    sut_topic = dict(native['samples']['DCPSTopic'][0], topic_name='rt/test',
                     type_name='std_msgs::msg::dds_::String_', topic_key='cd'*16)
    native['samples']['DCPSTopic'].append(sut_topic)
    for receipt in native['receipts']:
        if receipt['operation'] == 'take:DCPSTopic' and receipt['receipt']['samples']:
            receipt['receipt']['samples'].append(deepcopy(sut_topic))
            receipt['receipt']['take_return_code'] += 1
    parts[-1]['raw'] = jsonraw(native_path, native)
    binding = parts[2][0]
    lifecycle_path = Path(binding['return_raw_ref']['path'])
    lifecycle = [json.loads(line) for line in lifecycle_path.read_text().splitlines()]
    participant = PREFIX+'000001c1'
    lifecycle[1]['participant_ownership'] = {
        'schema': 'r2f.cyclone-participant-ownership/v2', 'complete': True, 'status': 'observed',
        'stable_sample_count': 2, 'candidate_eligible': False, 'local_gid': GUID,
        'participant_gid': participant, 'domain_id': native['domain_id'], 'role_kind': 'writer',
        'local_handle': 14, 'participant_handle': 10, 'parent_chain': [14, 10],
        'started_monotonic_ns': 1002, 'ended_monotonic_ns': 1015,
        'participant_root_observation': {'participant_handle': 10, 'participant_self_return': 10,
                                         'lookup_participants': [10]},
        'entity_identities': [{'handle': 14, 'gid': GUID}, {'handle': 10, 'gid': participant}],
        'provenance': {'abi': 'offline_fixture_no_execution'}}
    ref = hashed(lifecycle_path, b''.join(json.dumps(event).encode()+b'\n' for event in lifecycle))
    for key in ('begin_raw_ref', 'return_raw_ref'):
        binding[key].update(ref)
    observation = next(event for event in raw['events'] if event['event'] == 'discovery_python_observation')
    python_topic = dict(observation['samples']['DCPSTopic'][0], topic_name='rt/test',
                        type_name='std_msgs::msg::dds_::String_', key='cd'*16)
    observation['samples']['DCPSTopic'].append(python_topic)
    def full_result():
        reference = {'schema': raw['schema'], 'status': raw['status'],
                     'raw': jsonraw(tmp_path/'tools.json', raw)}
        return reconcile_discovery_projection(parts[0], parts[1], endpoint_bindings=parts[2],
            observation_window=parts[3], discovery_layers=parts[4], discovery_tools=reference)['schema_check']
    schema = full_result()
    assert schema['all_layer_obligations_closed'] is True, schema['observation_obligations']
    # The same fields advertised by an unrelated participant cannot close it.
    original_capture = Path(capture['raw']['path']).read_bytes()
    tail = pcap([(BASE+127, frame(packet(payload).replace(
        bytes.fromhex('0102030405060708090a0b0c'), bytes.fromhex(PREFIX))))])[24:]
    def rewrite_capture(data):
        capture['raw'] = hashed(Path(capture['raw']['path']), data)
        capture['receipt_raw'] = jsonraw(tmp_path/'capture.json',
            {key: value for key, value in capture.items() if key != 'receipt_raw'})
        parts[0] = parse_sedp_capture(capture)
    rewrite_capture(original_capture[:-len(tail)] + tail.replace(bytes.fromhex(PREFIX), bytes.fromhex('aa'*12)))
    assert full_result()['all_layer_obligations_closed'] is False
    rewrite_capture(original_capture)
    # A healthy control never masks a missing actual-topic binding observation.
    observation['samples']['DCPSTopic'].remove(python_topic)
    schema = full_result()
    assert schema['discovery_tools']['positive_control']['status'] == 'consistent'
    assert schema['all_layer_obligations_closed'] is False
    assert schema['observation_obligations']['review_required'] is True
