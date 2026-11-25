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
            topic_name='rt/test', type_name='std_msgs::msg::dds_::String_')],
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
