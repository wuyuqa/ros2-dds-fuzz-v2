"""Producer API regressions: offline calls only; never promote a fixture to DDS."""
from copy import deepcopy
import hashlib
import json
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

import pytest

from r2f.campaign import replay
from r2f.campaign.topology_adapter import facts_from_events
from r2f.core.evidence import canonical_hash
from r2f.modules.s1 import topology_model as model
from r2f.runtime import topology_pair, topology_route
from r2f.runtime.ros2_pair import run_pair
from r2f.runtime.topology_worker import OfflineTopologyBackend, RosTopologyBackend
from test_topology_runtime_route import config_for


ROS_PREPARE_WITHOUT_WORKER_LAUNCH = RosTopologyBackend.prepare


@pytest.fixture(autouse=True)
def no_ros(monkeypatch):
    monkeypatch.setattr('r2f.runtime.topology_worker.RosTopologyBackend.prepare',
                        Mock(side_effect=AssertionError('NO ROS/DDS in producer contracts')))


def read_raw(result):
    path = Path(result['raw_path'])
    data = path.read_bytes()
    assert hashlib.sha256(data).hexdigest() == result['raw_sha256']
    events = [json.loads(line) for line in data.splitlines()]
    assert events == result['events']
    assert [e['event_index'] for e in events] == list(range(len(events)))
    return events


def exact_result(root, result):
    stored = json.loads((root / 'runtime_result.json').read_text())
    assert stored == result
    assert canonical_hash(stored) == canonical_hash(result)
    assert not list(root.glob('.runtime_result-*.tmp'))
    return stored


def test_pair_final_cleanup_flag_is_raw_backed_and_last(tmp_path):
    result = topology_pair.run_topology(model.generate_program(7, writers=1, readers=1),
        {'backend': 'offline_fake'}, tmp_path, backend=OfflineTopologyBackend())
    assert result['runtime_status'] == 'completed'
    events = read_raw(result)
    end = events[-1]
    assert end['event'] == 'runtime_end' and end['cleanup_complete'] is True
    cleanup = next(e for e in events if e['event'] == 'topology_cleanup')
    closed = next(e for e in events if e['event'] == 'topology_backend_closed')
    exits = [e for e in events if e['event'] == 'topology_worker_exit']
    assert exits and all(e['event_index'] < closed['event_index'] for e in exits)
    assert closed['event_index'] < cleanup['event_index'] < end['event_index']
    assert cleanup['receipt'] == result['cleanup_receipt']
    receipt = result['precondition_receipts'][-1]
    assert receipt['step_id'] == 'cleanup' and receipt['satisfied'] is True
    assert result['real_execution'] is False and end['fixture'] is True


class BadCleanup(OfflineTopologyBackend):
    def __init__(self, damage):
        super().__init__()
        self.damage = damage

    def close(self):
        receipt = super().close()
        if isinstance(self.damage, Exception):
            raise self.damage
        if self.damage == 'malformed':
            return None
        receipt.update(deepcopy(self.damage))
        return receipt


@pytest.mark.parametrize('damage', [
    RuntimeError('cleanup broke'), TypeError('cleanup invalid'), 'malformed',
    {'closed': False}, {'closed': 1}, {'errors': ['close failed']},
    {'survivors': [123]}, {'error': 'close failed'}, {'returncodes': [0, 9]},
    {'returncodes': [None]}, {'workers': [{'closed': False, 'returncode': 0}]},
    {'workers': [{'closed': True, 'returncode': -9}]},
])
def test_failed_cleanup_cannot_end_successfully(tmp_path, damage):
    result = topology_pair.run_topology(model.generate_program(7, writers=1, readers=1),
        {'backend': 'offline_fake'}, tmp_path, backend=BadCleanup(damage))
    events = read_raw(result)
    assert result['runtime_status'] == result['status'] == 'aborted'
    assert events[-1]['cleanup_complete'] is False
    assert result['precondition_receipts'][-1]['satisfied'] is False
    assert any(e['event'] == 'runtime_abort' for e in events)


@pytest.mark.parametrize('code', [None, 7, -9])
def test_actual_process_poll_overrides_closed_receipt(tmp_path, code):
    backend = OfflineTopologyBackend()
    # A controlled process object: no subprocess is created by this test.
    backend.processes = [SimpleNamespace(process=SimpleNamespace(poll=lambda: code, pid=123))]
    result = topology_pair.run_topology(model.generate_program(7, writers=1, readers=1),
        {'backend': 'offline_fake'}, tmp_path, backend=backend)
    assert result['cleanup_receipt']['closed'] is True
    assert read_raw(result)[-1]['cleanup_complete'] is False
    assert result['runtime_status'] == 'aborted'


def test_final_route_result_is_persisted_after_all_enhancement(tmp_path):
    _, config = config_for()
    original_audit = topology_route.audit_topology_application
    observed = []

    def audit(*args, **kwargs):
        # Pair also invokes the audit before runtime_end; neither call may
        # publish a provisional runtime_result.json.
        observed.append(True)
        assert not (tmp_path / 'runtime_result.json').exists()
        return original_audit(*args, **kwargs)

    with patch.object(topology_route, 'audit_topology_application', side_effect=audit):
        result = run_pair(config, tmp_path, topology_backend=OfflineTopologyBackend())
    stored = exact_result(tmp_path, result)
    assert observed
    assert stored['route_binding']['effective_input_sha256'] == config['effective_input_sha256']
    assert stored['application_audit']['status'] == 'fixture_application_observed'
    assert stored['mutation_receipt'] == stored['application_audit']['mutation_receipt']
    assert stored['scenario_mutation_receipt']['reason'] == 'topology_backend_is_a_scenario_carrier_not_a_mutation'
    assert read_raw(result)[-1]['cleanup_complete'] is True
    assert result['fixture'] is True and result['real_execution'] is False
    assert not any(result['mutation_receipt'][k] for k in ('applied', 'hit', 'effect_observed'))
    assert not any(result[k] for k in ('candidate_eligible', 'sealed', 'mature'))


def test_actual_producer_meets_artifact_fields_but_never_real_qualification(tmp_path):
    _, config = config_for()
    result = topology_route.run_topology_route(config, tmp_path, backend=OfflineTopologyBackend())
    exact_result(tmp_path, result)
    facts = facts_from_events(read_raw(result), result, config)
    assert facts['diagnostic_valid'] is True, facts
    assert facts['endpoint_identity_complete'] is True
    assert facts['fixture'] is True and facts['execution_evidence'] is False
    qualified = replay._qualification(replay.DEFAULT_RUNTIME, config, result,
        {'actual_run': False, 'execution_stage': 'judged'}, output_root=tmp_path)
    assert qualified['real_ros_execution'] is False
    errors = qualified['qualification_errors']
    assert 'topology_runtime_artifact_missing' not in errors
    assert 'topology_runtime_artifact_root_or_result_mismatch' not in errors
    assert 'topology_cleanup_not_verified' not in errors
    assert 'topology_fixture_cannot_establish_real_execution' in errors


@pytest.mark.parametrize('damage', ['bad_hash', 'real_qualification'])
def test_rejected_route_leaves_no_artifact_or_execution(tmp_path, damage):
    _, config = config_for()
    if damage == 'bad_hash':
        config['effective_input_sha256'] = '0' * 64
    else:
        config['require_real_execution'] = True
    backend = OfflineTopologyBackend()
    with patch.object(backend, 'prepare', side_effect=AssertionError('blocked must not execute')):
        result = topology_route.run_topology_route(config, tmp_path, backend=backend)
    assert not list(tmp_path.iterdir())
    assert result['status'] == result['runtime_status'] == 'blocked'
    assert result['execution'] is None and result['execution_metadata'] is None
    assert result['raw_path'] is None and result['events'] == []
    assert not (tmp_path / 'raw.jsonl').exists()
    assert not any(result['mutation_receipt'].values())


class FailedPrepare(OfflineTopologyBackend):
    def prepare(self, *args):
        raise RuntimeError('controlled backend failure')


def test_backend_exception_persists_final_aborted_route(tmp_path):
    _, config = config_for()
    result = topology_route.run_topology_route(config, tmp_path, backend=FailedPrepare())
    exact_result(tmp_path, result)
    assert result['status'] == 'aborted'
    assert 'controlled backend failure' in result['reason']
    assert result['route_binding'] and result['application_audit']
    assert result['mutation_receipt'] == result['application_audit']['mutation_receipt']
    assert not result['real_execution']
    read_raw(result)


def test_audit_exception_never_publishes_pre_enhancement_result(tmp_path):
    _, config = config_for()
    original_audit = topology_route.audit_topology_application
    calls = []

    def fail_final(*args, **kwargs):
        calls.append(True)
        if 'events' in args[2]:
            raise RuntimeError('controlled final audit failure')
        return original_audit(*args, **kwargs)

    with patch.object(topology_route, 'audit_topology_application', side_effect=fail_final):
        with pytest.raises(RuntimeError, match='controlled final audit failure'):
            topology_route.run_topology_route(config, tmp_path, backend=OfflineTopologyBackend())
    assert calls and (tmp_path / 'raw.jsonl').exists()
    assert not (tmp_path / 'runtime_result.json').exists()


def test_persist_io_failure_propagates_without_partial_result(tmp_path):
    _, config = config_for()
    with patch.object(topology_route.os, 'replace', side_effect=OSError('controlled write failure')):
        with pytest.raises(OSError, match='controlled write failure'):
            topology_route.run_topology_route(config, tmp_path, backend=OfflineTopologyBackend())
    assert not (tmp_path / 'runtime_result.json').exists()
    assert not list(tmp_path.glob('.runtime_result-*.tmp'))


@pytest.mark.parametrize('damage', [
    {'active': True}, {'restore_failure': {'error': 'failed'}}, {'restore': None},
    {'restore': {'restored': False}},
    {'restore': {'restored': True, 'only_noqueue_root': True,
                 'residual_qdisc': ['netem'], 'residual_filter': []}},
])
def test_netem_cleanup_derivation_rejects_unrestored_or_residual(damage):
    # Supplemental pure cleanup gate test, not authored execution evidence.
    context = {'active': False, 'restore_failure': None,
               'restore': {'restored': True, 'only_noqueue_root': True,
                           'residual_qdisc': [], 'residual_filter': []}}
    backend = OfflineTopologyBackend()
    assert topology_pair._cleanup_is_complete({'closed': True}, backend, context)
    context.update(damage)
    assert not topology_pair._cleanup_is_complete({'closed': True}, backend, context)


def test_pair_early_no_live_go_keeps_raw_and_execution_absent(tmp_path):
    _, config = config_for()
    config['backend'] = 'ros2_public_api'
    result = topology_route.run_topology_route(config, tmp_path)
    assert not list(tmp_path.iterdir())
    assert result['reason'] == 'blocked:no_live_ros_go'
    assert result['status'] == 'blocked' and result['real_execution'] is False
    assert result['events'] == [] and result['raw_path'] is None
    assert result['execution'] is result['execution_metadata'] is None
    assert not (tmp_path / 'raw.jsonl').exists()
    assert result['application_audit']['execution_evidence'] is False
    assert not any(result['mutation_receipt'].values())


def test_netem_attempt_without_restoration_receipt_is_not_complete():
    assert not topology_pair._cleanup_is_complete(
        {'closed': True}, OfflineTopologyBackend(), None, netem_required=True)


def test_final_cleanup_flag_follows_lease_release_without_ros_workers(tmp_path):
    # Existing orchestration contract uses zero endpoints, measured-preflight
    # doubles and a create_endpoint guard: no ROS imports or worker launches.
    from test_topology_runtime_route import test_real_orchestration_mock_releases_only_after_cleanup
    original_persist = topology_route._persist_runtime_result
    captured = []

    def inspect_final(result, output):
        events = read_raw(result)
        cleanup = next(e for e in events if e['event'] == 'topology_cleanup')
        released = next(e for e in events if e['event'] == 'isolation_locks_released')
        assert cleanup['event_index'] < released['event_index'] < events[-1]['event_index']
        assert events[-1]['event'] == 'runtime_end' and events[-1]['cleanup_complete'] is True
        assert result['real_execution'] is False and result['candidate_eligible'] is False
        captured.append(True)
        return original_persist(result, output)

    with patch.object(RosTopologyBackend, 'prepare', ROS_PREPARE_WITHOUT_WORKER_LAUNCH), patch.object(
            topology_route, '_persist_runtime_result', side_effect=inspect_final):
        test_real_orchestration_mock_releases_only_after_cleanup(tmp_path)
    assert captured == [True]
