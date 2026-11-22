"""Offline run_pair regression: synthetic workers/time, never ROS or DDS evidence."""
import json
from pathlib import Path
from unittest.mock import Mock, patch

import pytest

from r2f.runtime.ros2_pair import run_pair


class Clock:
    def __init__(self):
        self.ns = 1_000_000_000

    def monotonic_ns(self):
        self.ns += 1
        return self.ns

    def monotonic(self):
        return self.monotonic_ns() / 1_000_000_000


class OfflinePool:
    """Replace only the worker transport boundary; controller gates stay real."""
    def __init__(self, config, environment, output_dir, emit, *, role_environments,
                 clock, fail_at=None):
        self.config, self.emit, self.clock = config, emit, clock
        self.fail_at = fail_at
        self.metadata, self.pending = [], []
        self.serial = 0

    def start(self, role):
        if self.fail_at == role:
            raise RuntimeError('offline_worker_failure:' + role)
        pid = 101 if role == 'writer' else 102
        created_ns = self.clock.monotonic_ns()
        if role == 'writer':
            self.writer_created_ns = created_ns
        elif self.fail_at == 'nonpositive_interval':
            created_ns = self.writer_created_ns
        self.emit('endpoint_created', role=role, pid=pid, endpoint_gid=role + '-gid',
                  created_monotonic_ns=created_ns,
                  rmw_identifier=self.config['rmw'], qos=self.config[role + '_qos'])
        self.emit('qos_binding_observed', role=role,
                  observed_self_gid=role + '-gid', observed_self=self.config[role + '_qos'])

    def command(self, role, command, **fields):
        self.serial += 1
        self.pending.append((role, command, self.serial, fields))
        return self.serial

    def wait(self, predicate, timeout, label):
        deadline = self.clock.monotonic() + timeout
        while not predicate():
            pending, self.pending = self.pending, []
            for role, command, cid, fields in pending:
                if self.fail_at == 'prejoin' and fields.get('phase') == 'prejoin':
                    raise TimeoutError('offline_prejoin_unacknowledged')
                if command == 'publish':
                    self.emit('send', role=role, **fields)
                    if fields['phase'] == 'post_match':
                        self.emit('receive', role='reader', **fields,
                                  payload_run_id=self.config['run_id'])
                elif command == 'snapshot':
                    self.emit('match_snapshot', role=role, command_id=cid, matched_count=1,
                              sampled_monotonic_ns=self.clock.monotonic_ns(),
                              endpoint_gid=role + '-gid')
                    self.emit('graph_snapshot', role=role, command_id=cid,
                              publishers=[{'endpoint_gid': 'writer-gid'}],
                              subscriptions=[{'endpoint_gid': 'reader-gid'}])
                else:
                    raise AssertionError('unexpected offline command: ' + command)
                self.emit('command_done', role=role, command_id=cid)
            self.clock.ns += 1_000_000
            if self.clock.monotonic() > deadline:
                raise TimeoutError(label)

    def close(self):
        pass


def invoke(output_dir, *, target='discovery.late_endpoint', arm='mutated',
           delay=0.15, fail_at=None):
    config = {'target_id': target, 'target_revision': 0, 'arm': arm,
              'campaign_kind': 'required', 'run_id': 'offline-late-joiner',
              'domain_id': 100, 'reader_delay_sec': delay,
              'baseline_reader_delay_sec': 0.15, 'pre_match_count': 2,
              'post_match_count': 6, 'publish_interval_sec': 0.03,
              'evidence_purpose': 'offline_synthetic_controller_test'}
    if target.endswith('.fuzz_v1'):
        config.update(campaign_kind='exploratory', fuzz_surfaces=['P01'],
                      submitted_parameters={'reader_delay_sec': delay})
    clock = Clock()
    context = Mock(base_env={}, role_env={})
    context.collect.return_value = {'status': 'incomplete', 'verdict': 'unknown',
                                    'intake': [], 'candidate_count': 0}
    def factory(*args, **kwargs):
        return OfflinePool(*args, **kwargs, clock=clock, fail_at=fail_at)
    with patch('r2f.runtime.ros2_pair.acquire_pair_environment',
               return_value=(None, {'offline_synthetic': True}, {})), \
         patch('r2f.runtime.instrumentation_bridge.prepare', return_value=context), \
         patch('r2f.runtime.ros2_pair.WorkerPool', side_effect=factory), \
         patch('r2f.runtime.ros2_pair.time.monotonic', clock.monotonic), \
         patch('r2f.runtime.ros2_pair.time.monotonic_ns', clock.monotonic_ns):
        result = run_pair(config, output_dir)
    events = [json.loads(line) for line in Path(result['raw_path']).read_text().splitlines()]
    return result, events


@pytest.mark.parametrize('target', ['discovery.late_endpoint', 'discovery.late_join_delivery'])
def test_required_positive_delay_records_effect_without_fuzz_surfaces(tmp_path, target):
    result, events = invoke(tmp_path / 'required', target=target)
    assert result['runtime_status'] == 'completed', result['reason']
    assert result['mutation_receipt']['applied'] is True
    assert result['mutation_receipt']['hit'] is True
    assert result['mutation_receipt']['effect_observed'] is True
    assert not any(e['event'] == 'mutation_no_effect' for e in events)

    begin = next(e for e in events if e['event'] == 'mutation_begin')
    reader = next(e for e in events if e['event'] == 'endpoint_created' and e['role'] == 'reader')
    effect = next(e for e in events if e['event'] == 'mutation_effect')
    assert begin['preconditions_snapshot']['reader_created'] is False
    assert begin['preconditions_snapshot']['prejoin_publish_sequences'] == [0, 1]
    assert [r['step_id'] for r in result['precondition_receipts']] == [
        'writer_created', 'writer_prejoin_published']
    assert all(r['satisfied'] for r in result['precondition_receipts'])
    for ref in begin['precondition_receipt_refs']:
        assert events[ref['raw_before_index']]['event'] == 'state_step_before'
        assert events[ref['raw_after_index']]['event'] == 'state_step_after'
        assert ref['raw_before_index'] < ref['raw_after_index'] < begin['event_index']
    assert begin['event_index'] < reader['event_index'] < effect['event_index']
    assert effect['configured_delay_sec'] == 0.15
    assert effect['observed_delay_ns'] > 150_000_000
    assert not any(e['event'] == 'control_begin' for e in events)


@pytest.mark.parametrize('target', ['discovery.late_endpoint.fuzz_v1',
                                    'discovery.late_join_delivery.fuzz_v1'])
def test_exploratory_positive_delay_keeps_binding_and_effect(tmp_path, target):
    result, events = invoke(tmp_path / 'exploratory', target=target)
    assert result['runtime_status'] == 'completed', result['reason']
    assert result['mutation_receipt']['hit'] is True
    assert result['mutation_receipt']['effect_observed'] is True
    binding = next(e for e in events if e['event'] == 'exploratory_binding_observed')
    assert binding['observed']['P01']['configured_delay_sec'] == 0.15
    assert binding['observed']['P01']['observed_delay_ns'] > 150_000_000


@pytest.mark.parametrize('target', ['discovery.late_endpoint', 'discovery.late_join_delivery',
                                    'discovery.late_endpoint.fuzz_v1',
                                    'discovery.late_join_delivery.fuzz_v1'])
def test_zero_configured_delay_is_no_effect_despite_worker_elapsed_time(tmp_path, target):
    result, events = invoke(tmp_path / 'zero', target=target, delay=0)
    assert result['runtime_status'] == 'completed', result['reason']
    assert result['mutation_receipt']['applied'] is True
    assert result['mutation_receipt']['hit'] is False
    assert result['mutation_receipt']['effect_observed'] is False
    effect = next(e for e in events if e['event'] == 'mutation_no_effect')
    assert effect['configured_delay_sec'] == 0
    assert effect['observed_delay_ns'] > 0
    assert effect['reason'] == 'zero_delay_has_no_observable_timing_effect'
    assert not any(e['event'] in ('control_begin', 'mutation_effect') for e in events)


@pytest.mark.parametrize('arm', ['baseline', 'transparent'])
@pytest.mark.parametrize('target', ['discovery.late_endpoint', 'discovery.late_join_delivery',
                                    'discovery.late_endpoint.fuzz_v1'])
def test_controls_keep_positive_baseline_delay_without_mutation_hit(tmp_path, arm, target):
    result, events = invoke(tmp_path / 'control', arm=arm, target=target)
    assert result['runtime_status'] == 'completed', result['reason']
    assert not any(result['mutation_receipt'].values())
    assert any(e['event'] == 'control_begin' for e in events)
    assert not any(e['event'] in ('mutation_begin', 'mutation_effect', 'mutation_no_effect')
                   for e in events)
    interval = next(e for e in events if e['event'] == 'reader_created_after_writer')
    assert interval['configured_delay_sec'] == 0.15
    assert interval['delay_ns'] > 150_000_000
    if arm == 'transparent':
        control = next(e for e in events if e['event'] == 'transparency_control')
        assert control['mutation_hit'] is False
        assert control['applied_input_equal_to_baseline'] is True


@pytest.mark.parametrize('failure,reason,began', [
    ('writer', 'offline_worker_failure:writer', False),
    ('prejoin', 'offline_prejoin_unacknowledged', False),
    ('reader', 'offline_worker_failure:reader', True),
    ('nonpositive_interval', 'reader_not_created_after_writer', True),
])
def test_incomplete_prerequisites_or_timing_cannot_claim_hit(tmp_path, failure, reason, began):
    result, events = invoke(tmp_path / 'failure', fail_at=failure)
    assert result['runtime_status'] in ('blocked', 'failed')
    assert result['reason'] == reason
    assert result['mutation_receipt']['applied'] is False
    assert result['mutation_receipt']['hit'] is False
    assert result['mutation_receipt']['effect_observed'] is False
    assert any(e['event'] == 'mutation_begin' for e in events) is began
    assert not any(e['event'] in ('mutation_effect', 'mutation_no_effect') for e in events)
