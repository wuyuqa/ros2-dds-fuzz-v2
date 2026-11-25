"""Issue #13 CP3: persisted contracts and labelled external SDK doubles only."""
import json

import pytest

from r2f.runtime.topology_pair import run_topology
from test_topology_pair import CFG, action, endpoint, program


def lookup_program(scope='participant', name='rt/r2f/topology/a'):
    return program([endpoint('w0', 'writer')], [
        action('create', 'create', 'w0', 1),
        action('lookup', 'lookup_topic', 'w0', 1, scope=scope, name=name),
    ])


def test_lookup_input_reaches_persisted_receipt_without_fabricating_native_results(tmp_path):
    result = run_topology(lookup_program(), CFG, tmp_path)
    assert result['status'] == 'executed'
    rows = [json.loads(line) for line in (tmp_path / 'raw.jsonl').read_text().splitlines()]
    row = next(row for row in rows if row['event'] == 'topic_lookup_observation')
    receipt = row['lookup_receipt']
    assert receipt['status'] == 'unsupported'
    assert receipt['reason'] == 'offline_fixture_no_native_topic_lookup'
    assert receipt['scope'] == 'participant'
    assert receipt['requested_name'] == 'rt/r2f/topology/a'
    assert receipt['candidate_eligible'] is False
    assert receipt['execution_evidence'] is False
    assert result['real_execution'] is False
    assert any(r.get('action', {}).get('op') == 'lookup_topic' for r in result['precondition_receipts'])


def native_fixture():
    """External C ABI double: one owned endpoint and one found topic reference."""
    import ctypes as c
    from types import SimpleNamespace
    calls = []
    def typed(entity, out):
        assert entity == 35
        out._obj.value = 1234
        return 0
    def find(scope, participant, name, type_info, timeout):
        calls.append(('find', scope, participant, name, type_info.value, timeout))
        return 40
    def string(value):
        def read(entity, buf, size):
            buf.value = value
            return len(value)
        return read
    def delete(entity):
        calls.append(('delete', entity))
        return 0
    def free(ptr):
        calls.append(('free', ptr.value))
        return 0
    lib = SimpleNamespace(dds_get_topic=lambda entity: 35, dds_get_typeinfo=typed, dds_free_typeinfo=free,
                          dds_find_topic=find, dds_delete=delete,
                          dds_get_name=string(b'rt/r2f/topology/a'),
                          dds_get_type_name=string(b'std_msgs::msg::dds_::String_'),
                          dds_get_participant=lambda handle: 10)
    ownership = {'complete': True, 'status': 'observed', 'local_handle': 30,
                 'participant_handle': 10, 'participant_gid': 'a' * 32,
                 'local_gid': 'b' * 32, 'domain_id': 191}
    observer = SimpleNamespace(c=c, lib=lib,
        provenance={'abi': 'CycloneDDS-0.10.5-lp64-endpoint64', 'fixture': True},
        sample_ownership=lambda *args: dict(ownership))
    return observer, lib, calls


def test_native_typed_lookup_retains_participant_scope_and_releases_result():
    from r2f.runtime.scoped_topic_lookup import observe_topic_lookup
    native, _, calls = native_fixture()
    receipt = observe_topic_lookup(native, 191, 'b' * 32, 'writer',
                                   'participant', 'rt/r2f/topology/a')
    assert receipt['status'] == 'found' and receipt['complete'] is True
    assert receipt['api'] == 'dds_find_topic(scope,participant,name,type_info,timeout)'
    assert receipt['participant_gid'] == 'a' * 32
    assert receipt['result_participant_handle'] == 10
    assert receipt['actual_type_name'] == 'std_msgs::msg::dds_::String_'
    assert calls == [('find', 2, 10, b'rt/r2f/topology/a', 1234, 0),
                     ('delete', 40), ('free', 1234)]
    assert receipt['cleanup_confirmed'] is True
    assert receipt['candidate_eligible'] is False


def test_public_worker_persists_native_lookup_with_endpoint_and_command_identity(monkeypatch):
    import sys
    from r2f.runtime import topology_worker as worker
    from test_topology_participant_control_worker import ros_fixture
    native, _, calls = native_fixture()
    monkeypatch.setattr(worker, 'CycloneMatchedPeerObserver', lambda: native)
    cfg = {**CFG, **endpoint('w0', 'writer'), 'generation': 1}
    cfg['qos'] = worker.endpoint_expressibility(endpoint('w0', 'writer'), {})['public_qos']
    commands = [{'command': 'lookup_topic', 'command_id': 7,
                 'scope': 'participant', 'name': 'rt/r2f/topology/a'},
                {'command': 'stop', 'command_id': 8}]
    with ros_fixture(monkeypatch, commands) as (_, _, stream):
        monkeypatch.setattr(sys, 'argv', ['worker', json.dumps(cfg), 'writer'])
        assert worker.worker_main() == 0, stream.getvalue()
    rows = [json.loads(line) for line in stream.getvalue().splitlines()]
    found = next(row for row in rows if row['event'] == 'topic_lookup_result')
    created = next(row for row in rows if row['event'] == 'endpoint_created')
    assert found['command_id'] == 7
    assert found['lookup_receipt']['status'] == 'found'
    for key in ('logical_id', 'generation', 'endpoint_generation', 'run_id', 'pid'):
        assert found[key] == created[key]
    assert found['endpoint_gid'] == created['endpoint_gid']
    assert calls[-2:] == [('delete', 40), ('free', 1234)]
    ack = next(row for row in rows if row['event'] == 'command_done' and row['command_id'] == 7)
    for key in ('logical_id', 'generation', 'endpoint_generation', 'endpoint_gid',
                'run_id', 'pid', 'pgid', 'process_start_ticks'):
        assert ack.get(key) == found[key]
    assert rows.index(ack) > rows.index(found)


def test_missing_modern_api_is_unsupported_and_never_calls_deprecated_scoped_api():
    from r2f.runtime.scoped_topic_lookup import observe_topic_lookup
    native, lib, calls = native_fixture()
    del lib.dds_find_topic
    lib.dds_find_topic_scoped = lambda *args: calls.append(('deprecated',))
    receipt = observe_topic_lookup(native, 191, 'b' * 32, 'writer', 'global', 'rt/remote')
    assert receipt['status'] == 'unsupported'
    assert receipt['reason'] == 'topic_lookup_api_unavailable:dds_find_topic'
    assert receipt['complete'] is False and calls == []


@pytest.mark.parametrize('scope,code', [('participant', 2), ('local_domain', 1), ('global', 0)])
def test_scope_is_forwarded_to_modern_api_without_graph_or_remote_origin_claim(scope, code):
    from r2f.runtime.scoped_topic_lookup import observe_topic_lookup
    native, _, calls = native_fixture()
    receipt = observe_topic_lookup(native, 191, 'b' * 32, 'writer', scope, 'rt/r2f/topology/a')
    assert receipt['status'] == 'found'
    assert calls[0][1] == code
    assert receipt['result_ownership'] == 'requesting_participant_not_remote_origin'
    assert receipt['historical_validation'] == 'not_run'


@pytest.mark.parametrize('rc,status', [(0, 'not_found'), (-2, 'unsupported'), (-3, 'unknown')])
def test_absence_and_api_errors_are_separate_and_do_not_delete_borrowed_entities(rc, status):
    from r2f.runtime.scoped_topic_lookup import observe_topic_lookup
    native, lib, calls = native_fixture()
    lib.dds_find_topic = lambda *args: rc
    receipt = observe_topic_lookup(native, 191, 'b' * 32, 'writer', 'participant', 'rt/absent')
    assert receipt['status'] == status
    assert receipt['complete'] is (rc == 0)
    assert receipt['find_return_code'] == rc
    assert calls == [('free', 1234)]
    assert receipt['candidate_eligible'] is False


@pytest.mark.parametrize('damage', ['name', 'type', 'participant', 'ownership_race', 'delete', 'free'])
def test_contract_observation_and_resource_failures_keep_attribution_and_cleanup(damage):
    from r2f.runtime.scoped_topic_lookup import observe_topic_lookup
    native, lib, calls = native_fixture()
    if damage in ('name', 'type'):
        api = 'dds_get_name' if damage == 'name' else 'dds_get_type_name'
        original = getattr(lib, api)
        def wrong(entity, buffer, capacity):
            if entity == 40:
                buffer.value = b'wrong'
                return 5
            return original(entity, buffer, capacity)
        setattr(lib, api, wrong)
    elif damage == 'participant':
        lib.dds_get_participant = lambda *args: 20
    elif damage == 'ownership_race':
        original = native.sample_ownership
        count = 0
        def raced(*args):
            nonlocal count
            count += 1
            return {**original(*args), 'participant_gid': ('c' if count > 1 else 'a') * 32}
        native.sample_ownership = raced
    else:
        api = 'dds_delete' if damage == 'delete' else 'dds_free_typeinfo'
        setattr(lib, api, lambda *args: -1)
    receipt = observe_topic_lookup(native, 191, 'b' * 32, 'writer', 'global', 'rt/r2f/topology/a')
    if damage in ('name', 'type', 'participant'):
        assert receipt['status'] == 'contract_mismatch'
        assert receipt['attribution'] == 'type_or_scope_contract_requires_review'
    elif damage == 'ownership_race':
        assert receipt['status'] == 'unknown' and receipt['complete'] is False
        assert receipt['reason'] == 'topic_lookup_ownership_changed'
    else:
        assert receipt['status'] == 'tool_error' and receipt['complete'] is False
        assert receipt['cleanup_confirmed'] is False
    assert receipt['candidate_eligible'] is False
    assert ('free', 1234) in calls or damage == 'free'
    assert ('delete', 40) in calls or damage == 'delete'


@pytest.mark.parametrize('scope,name', [('invalid', 'rt/a'), ('participant', ''),
                                        ('participant', 'a\x00b'), ([], 'rt/a'),
                                        ('global', None)])
def test_malformed_lookup_inputs_are_rejected_before_backend(scope, name):
    from r2f.modules.s1.topology_model import validate_program
    assert validate_program(lookup_program(scope, name))['status'] == 'invalid'


def test_lookup_requires_live_current_generation_in_public_runner(tmp_path):
    p = lookup_program()
    p['actions'].insert(1, action('destroy', 'destroy', 'w0', 1))
    result = run_topology(p, CFG, tmp_path)
    assert not any(row.get('kind') == 'typed_topic_lookup' for row in result['observation_receipts'])
    assert any(row.get('code') == 'lookup_target_alive_guard' for row in result['precondition_receipts'])


def test_controlled_participant_input_retains_lookup_in_existing_plan_and_runner(tmp_path):
    from test_topology_participant_control_input import controlled
    from r2f.modules.s1.topology_model import validate_program
    p = controlled()
    p['actions'].append(action('lookup', 'lookup_topic', 'w0', 1,
                               scope='local_domain', name='rt/r2f/topology/a'))
    assert validate_program(p)['status'] == 'valid'
    result = run_topology(p, CFG, tmp_path)
    assert result['status'] == 'executed'
    assert any(row.get('kind') == 'typed_topic_lookup' for row in result['observation_receipts'])


def test_group_worker_routes_lookup_to_exact_endpoint_session(monkeypatch):
    from itertools import count
    from types import SimpleNamespace
    from r2f.runtime import topology_worker as worker
    from test_topology_participant_control_worker import run_worker, create
    numbers = count(1)
    monkeypatch.setattr(worker.uuid, 'uuid4', lambda: SimpleNamespace(hex=f'{next(numbers):032x}'))
    native, _, _ = native_fixture()
    monkeypatch.setattr(worker, 'CycloneMatchedPeerObserver', lambda: native)
    rc, _, rows = run_worker(monkeypatch, [create('w0', 1), create('r0', 2),
        {'command': 'lookup_topic', 'command_id': 3, 'logical_id': 'w0', 'generation': 1,
         'endpoint_generation': f'{2:032x}', 'scope': 'participant', 'name': 'rt/r2f/topology/a'},
        {'command': 'stop', 'command_id': 4}])
    assert rc == 0, rows
    lookup = next(row for row in rows if row['event'] == 'topic_lookup_result')
    assert lookup['logical_id'] == 'w0' and lookup['command_id'] == 3
    assert lookup['lookup_receipt']['status'] == 'found'
    assert lookup['group_id'] == 'group_a'


@pytest.mark.parametrize('damage', [None, 'generation', 'endpoint_gid', 'scope', 'duplicate', 'ack_identity', 'early_ack'])
def test_backend_collects_only_one_request_bound_worker_lookup(tmp_path, damage):
    from copy import deepcopy
    from r2f.runtime.topology_worker import RosTopologyBackend, TopologyBackendError
    identity = {'logical_id': 'w0', 'generation': 1, 'endpoint_generation': 'nonce',
                'endpoint_gid': 'b' * 32, 'pid': 100, 'pgid': 100, 'process_start_ticks': 900}
    class WorkerPipe:
        def __init__(self): self.events = []
        def command(self, command, **fields):
            assert command == 'lookup_topic'
            row = {**identity, 'run_id': CFG['run_id'], 'event': 'topic_lookup_result', 'command_id': 3,
                   'lookup_receipt': {'scope': fields['scope'], 'requested_name': fields['name'],
                                      'status': 'not_found', 'complete': True}}
            if damage in ('generation', 'endpoint_gid'): row[damage] = 'wrong'
            elif damage == 'scope': row['lookup_receipt']['scope'] = 'global'
            self.events.append(row)
            if damage == 'duplicate': self.events.append(deepcopy(row))
            ack = {**identity, 'event': 'command_done', 'command_id': 3}
            if damage == 'ack_identity': ack['endpoint_gid'] = 'wrong'
            if damage == 'early_ack': self.events.insert(0, ack)
            else: self.events.append(ack)
            return 3
        def has(self, event, **fields):
            return any(row['event'] == event and all(row.get(k) == v for k, v in fields.items())
                       for row in self.events)
        def wait(self, predicate, *args): assert predicate()
    backend = RosTopologyBackend(CFG, tmp_path)
    handle = {**identity, 'process': WorkerPipe()}
    if damage:
        with pytest.raises(TopologyBackendError): backend.lookup_topic(handle, 'participant', 'rt/a')
    else:
        receipt = backend.lookup_topic(handle, 'participant', 'rt/a')
        assert receipt['status'] == 'not_found'
        assert receipt['worker_identity'] == identity and receipt['command_id'] == 3


def test_lookup_stays_diagnostic_through_canonical_raw_fact_oracle_route(tmp_path):
    from test_topology_no_obligation_facts import produce, consume
    p = lookup_program('global')
    runtime, config = produce(tmp_path, program=p)
    facts, oracle = consume(runtime, config)
    row = next(row for row in runtime['events'] if row['event'] == 'topic_lookup_observation')
    assert row['lookup_receipt']['status'] == 'unsupported'
    assert not facts['candidate_eligible'] and not facts['seal_eligible']
    assert oracle['diagnostic_only']


@pytest.mark.parametrize('damage', ['version', 'typeinfo_unsupported', 'typeinfo_null'])
def test_inapplicable_native_binding_keeps_named_reason_without_sut_claim(damage):
    from r2f.runtime.scoped_topic_lookup import observe_topic_lookup
    native, lib, calls = native_fixture()
    if damage == 'version': native.provenance['abi'] = 'unverified-newer-abi'
    elif damage == 'typeinfo_unsupported': lib.dds_get_typeinfo = lambda *args: -2
    else: lib.dds_get_typeinfo = lambda *args: 0
    receipt = observe_topic_lookup(native, 191, 'b' * 32, 'writer', 'global', 'rt/a')
    assert receipt['complete'] is False and receipt['candidate_eligible'] is False
    assert receipt['status'] == ('unknown' if damage == 'typeinfo_null' else 'unsupported')
    assert calls == []


@pytest.mark.parametrize('failure,status,attribution', [
    (OSError('fixture_maps_io'), 'unknown', 'binding_adaptation_or_observation_error'),
    (ValueError('fixture_invalid_maps'), 'unknown', 'binding_adaptation_or_observation_error'),
    (ValueError('native_endpoint_c_abi_platform_unsupported'), 'unsupported', 'capability_unsupported_not_sut_defect'),
])
def test_worker_distinguishes_adapter_failure_from_named_capability_rejection(
        monkeypatch, failure, status, attribution):
    import sys
    from r2f.runtime import topology_worker as worker
    from test_topology_participant_control_worker import ros_fixture
    def failed_observer(): raise failure
    monkeypatch.setattr(worker, 'CycloneMatchedPeerObserver', failed_observer)
    cfg = {**CFG, **endpoint('w0', 'writer'), 'generation': 1}
    cfg['qos'] = worker.endpoint_expressibility(endpoint('w0', 'writer'), {})['public_qos']
    commands = [{'command': 'lookup_topic', 'command_id': 7, 'scope': 'participant', 'name': 'rt/a'},
                {'command': 'stop', 'command_id': 8}]
    with ros_fixture(monkeypatch, commands) as (_, _, stream):
        monkeypatch.setattr(sys, 'argv', ['worker', json.dumps(cfg), 'writer'])
        assert worker.worker_main() == 0, stream.getvalue()
    rows = [json.loads(line) for line in stream.getvalue().splitlines()]
    lookup = next(row['lookup_receipt'] for row in rows if row['event'] == 'topic_lookup_result')
    assert lookup['status'] == status and lookup['attribution'] == attribution
    assert str(failure) in lookup['reason']
    assert lookup['exception_type'] == type(failure).__name__
    assert lookup['candidate_eligible'] is False


def test_bound_topic_without_type_information_is_unsupported_not_sut_absence():
    from r2f.runtime.scoped_topic_lookup import observe_topic_lookup
    native, lib, calls = native_fixture()
    # Cyclone 0.10.5 ddsrt/retcode.h: DDS_XRETCODE(10) == -60.
    lib.dds_get_typeinfo = lambda *args: -60
    receipt = observe_topic_lookup(native, 191, 'b' * 32, 'writer',
                                   'participant', 'rt/r2f/topology/a')
    assert receipt['status'] == 'unsupported'
    assert receipt['reason'] == 'topic_lookup_bound_type_info_unavailable'
    assert receipt['attribution'] == 'capability_unsupported_not_sut_defect'
    assert receipt['type_info_return_code'] == -60
    assert receipt['complete'] is False and receipt['candidate_eligible'] is False
    assert receipt['cleanup_confirmed'] is True
    assert receipt['ownership_before'] == receipt['ownership_after']
    # No fallback to name-only lookup and no release of borrowed references.
    assert calls == []
