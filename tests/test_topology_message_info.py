"""Optional public callback fields; no ROS imports, nodes or real DDS claims."""
import ast
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from r2f.runtime.message_info_receipt import capture_message_info, UINT64_MAX


@pytest.mark.parametrize('kind', [dict, SimpleNamespace])
def test_dict_and_attribute_fields_are_captured_without_coercion(kind):
    values = dict(source_timestamp=1020, received_timestamp=1030,
                  publication_sequence_number=0, reception_sequence_number=8)
    r = capture_message_info(kind(**values))
    assert {k: f['value'] for k, f in r['fields'].items()} == values
    assert all(f['status'] == 'observed' for f in r['fields'].values())
    assert not r['identity_qualified'] and not r['candidate_eligible']
    assert r['filter_effect_observed'] is None
    assert json.loads(json.dumps(r)) == r


@pytest.mark.parametrize('info', [{}, None, SimpleNamespace(), {'source_timestamp': None}])
def test_missing_info_never_uses_payload_or_local_receive_sequence(info):
    r = capture_message_info(info)
    assert all(f == {'status': 'unavailable_api', 'value': None} for f in r['fields'].values())
    assert not r['identity_qualified']


@pytest.mark.parametrize('name', ['publication_sequence_number', 'reception_sequence_number'])
def test_rmw_unsupported_is_not_a_huge_observed_sequence(name):
    r = capture_message_info({name: UINT64_MAX})
    assert r['fields'][name] == {'status': 'unsupported_rmw', 'value': None, 'raw_value': UINT64_MAX}


@pytest.mark.parametrize('name', ['source_timestamp', 'received_timestamp'])
def test_zero_timestamp_retains_unknown_clock_not_measured_time(name):
    r = capture_message_info({name: 0})
    assert r['fields'][name] == {'status': 'zero_unavailable_or_unspecified', 'value': None, 'raw_value': 0}


@pytest.mark.parametrize('value', [True, False, 1.0, '1', -1, UINT64_MAX + 1, [], {}])
def test_malformed_fields_do_not_become_receipt_values(value):
    r = capture_message_info(dict(source_timestamp=value, received_timestamp=value,
                                 publication_sequence_number=value, reception_sequence_number=value))
    assert all(f['status'] == 'invalid_value' and f['value'] is None for f in r['fields'].values())


def test_descriptor_error_is_observable_and_does_not_abort_callback():
    class BadInfo:
        @property
        def source_timestamp(self):
            raise RuntimeError('do not include property content in the receipt')
    r = capture_message_info(BadInfo())
    assert r['fields']['source_timestamp'] == {'status': 'read_error', 'value': None, 'error_type': 'RuntimeError'}
    assert r['fields']['publication_sequence_number']['status'] == 'unavailable_api'


def test_unique_publication_identity_is_not_inferred_even_with_gid_looking_payload():
    info = dict(publication_sequence_number=7, reception_sequence_number=9,
                publisher_gid='aa'*16, writer_id='w0', send_sequence=99)
    r = capture_message_info(info)
    assert not r['identity_qualified']
    assert set(r['fields']) == {'source_timestamp', 'received_timestamp',
                              'publication_sequence_number', 'reception_sequence_number'}
    assert 'publisher_gid' not in json.dumps(r)


def receive_callback():
    """Execute the production nested callback against synthetic args, not ROS."""
    path = Path(__file__).resolve().parents[1]/'src/r2f/runtime/topology_worker.py'
    tree = ast.parse(path.read_text())
    worker = next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == '_worker_main')
    original = next(n for n in worker.body if isinstance(n, ast.FunctionDef) and n.name == 'receive')
    wrapper = ast.parse('def wrapper():\n receive_sequence = 0\n pass\n return receive\n').body[0]
    wrapper.body[1] = original
    emitted = []
    from topology_ros_message_fixture import bound_message_codec
    codec = bound_message_codec()
    env = {'codec': codec, '__name__': 'r2f.runtime.topology_worker', '__package__': 'r2f.runtime',
           'json': json, 'local_gid': 'reader-gid',
           'gid_bytes': lambda x: bytes(x).hex() if x is not None else None,
           'emit': lambda name, **fields: emitted.append(dict(event=name, **fields))}
    exec(compile(ast.fix_missing_locations(ast.Module(body=[wrapper], type_ignores=[])), str(path), 'exec'), env)
    return env['wrapper'](), emitted, codec.make_message


def test_real_callback_wiring_captures_only_supplied_public_fields():
    receive, rows, make_message = receive_callback()
    data = json.dumps({'run_id': 'fixture', 'writer_id': 'w0', 'writer_generation': 1,
                       'send_sequence': 15, 'phase': 'pre'})
    info = dict(publication_sequence_number=17, reception_sequence_number=21,
                source_timestamp=3000, received_timestamp=4000)
    receive(make_message(data), info)
    receive(make_message(data), dict(info, publication_sequence_number=18, reception_sequence_number=22))
    assert [r['receive_sequence'] for r in rows] == [1, 2]
    assert [r['send_sequence'] for r in rows] == [15, 15]  # same complete bytes, distinct captured transport numbers
    assert [r['message_info_receipt']['fields']['publication_sequence_number']['value'] for r in rows] == [17, 18]
    assert all(r['publisher_gid'] is None and r['publisher_gid_status'] == 'unavailable_api' for r in rows)
    assert all(not r['message_info_receipt']['identity_qualified'] for r in rows)
    assert rows[0]['payload_sha256'] == rows[1]['payload_sha256']
