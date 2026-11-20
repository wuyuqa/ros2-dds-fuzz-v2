"""Synthetic SDK doubles only; no participant, ROS context or native DDS call."""
import ast
import inspect
from types import SimpleNamespace

import pytest

from r2f.runtime import topology_worker as worker


CLOCK = {'boot_id': 'fixture-boot', 'device': 5, 'inode': 123}


class Reader:
    def __init__(self, counts=(1, 1)):
        self.counts = iter(counts)

    def get_publisher_count(self):
        return next(self.counts)


class NativeObserver:
    def sample(self, domain, gid, kind):
        return {'complete': True, 'domain_id': domain, 'local_gid': gid,
                'role_kind': kind, 'matched_peer_gids': ['writer-gid'],
                'provenance': {'kind': 'fixture_sdk_double'}}


def sampler(monkeypatch, endpoint=None, rmw='rmw_cyclonedds_cpp'):
    monkeypatch.setattr(worker, '_monotonic_clock_identity', lambda: dict(CLOCK))
    monkeypatch.setattr(worker, 'CycloneMatchedPeerObserver', NativeObserver)
    return worker._LivelinessPeerSampler(endpoint or Reader(), rmw, 191, 'reader-gid')


def test_read_native_peers_with_counts_is_only_a_bounded_sample(monkeypatch):
    observed = sampler(monkeypatch)()
    assert observed['status'] == 'observed_sample'
    assert observed['matched_peer_gids'] == ['writer-gid']
    assert observed['peer_gids_complete'] is True
    assert observed['public_count_before'] == observed['public_count_after'] == 1
    assert observed['sample_started_ns'] <= observed['sample_finished_ns']
    assert observed['sample_clock_identity'] == CLOCK
    assert observed['scope'] == 'bounded_peer_sample_not_continuous'
    assert observed['continuous_coverage'] is False
    assert observed['external_peers_known'] is None
    assert observed['effect_observed'] is None
    assert observed['candidate_eligible'] is False


def test_racing_public_count_cannot_claim_complete_peer_sample(monkeypatch):
    observed = sampler(monkeypatch, Reader((1, 2)))()
    assert observed['status'] == 'unknown'
    assert observed['peer_gids_complete'] is False
    assert not observed['continuous_coverage']


@pytest.mark.parametrize('counts', [(True, 1), (1, True), (-1, -1), (None, None)])
def test_bad_public_counts_fail_closed(monkeypatch, counts):
    observed = sampler(monkeypatch, Reader(counts))()
    assert observed['status'] == 'unknown'
    assert not observed['peer_gids_complete']


def test_other_rmw_keeps_aggregate_sample_but_not_native_peer_claim(monkeypatch):
    observed = sampler(monkeypatch, rmw='rmw_fastrtps_cpp')()
    assert observed['public_count_before'] == observed['public_count_after'] == 1
    assert observed['status'] == 'unknown'
    assert observed['reason'] == 'rmw_has_no_public_peer_observer'
    assert not observed['peer_gids_complete']


def test_sampler_failure_keeps_callback_and_audits_failure(monkeypatch):
    class Broken:
        def __call__(self):
            raise RuntimeError('fixture failure')
    records = []
    capture = worker._LivelinessEventCapture('reader', lambda event, **kw:
                                            records.append({'event': event, **kw}))
    capture.bind('reader-gid')
    capture.peer_sampler = Broken()
    capture.callback(SimpleNamespace(alive_count=0, not_alive_count=1,
                                    alive_count_change=-1, not_alive_count_change=1))
    assert [r['event'] for r in records] == ['topology_liveliness_event',
                                            'topology_liveliness_peer_snapshot']
    assert records[0]['status_fields']['not_alive_count'] == 1
    assert records[1]['callback_id'] == records[0]['callback_id'] == 1
    assert records[1]['reader_gid'] == 'reader-gid'
    assert records[1]['status'] == 'unknown'
    assert records[1]['reason'] == 'peer_sampler_exception:RuntimeError'
    assert records[1]['continuous_coverage'] is False


def test_native_exception_preserves_unknown_and_public_counts(monkeypatch):
    observed = sampler(monkeypatch)
    class BrokenObserver:
        def sample(self, *_):
            raise OSError('fixture')
    observed.observer = BrokenObserver()
    value = observed()
    assert value['status'] == 'unknown'
    assert value['reason'] == 'native_peer_observation_failed:OSError'
    assert value['public_count_before'] == value['public_count_after'] == 1


def test_callback_and_sample_are_bound_without_synthesizing_coverage(monkeypatch):
    records = []
    capture = worker._LivelinessEventCapture('reader', lambda event, **kw:
                                            records.append({'event': event, **kw}))
    capture.bind('reader-gid')
    capture.peer_sampler = sampler(monkeypatch, Reader((1, 1, 1, 1)))
    for alive in (0, 1):
        capture.callback(SimpleNamespace(alive_count=alive, not_alive_count=1-alive,
                                        alive_count_change=2*alive-1,
                                        not_alive_count_change=1-2*alive))
    callbacks = [r for r in records if r['event'] == 'topology_liveliness_event']
    samples = [r for r in records if r['event'] == 'topology_liveliness_peer_snapshot']
    assert [r['callback_id'] for r in callbacks] == [1, 2]
    assert [r['callback_id'] for r in samples] == [1, 2]
    assert all(r['callback_monotonic_ns'] == e['callback_monotonic_ns']
               for r, e in zip(samples, callbacks))
    assert all(r['sample_started_ns'] >= r['callback_monotonic_ns'] for r in samples)
    assert not any(r['event'] == 'topology_liveliness_peer_coverage' for r in records)


def test_early_and_cleanup_callbacks_never_query_endpoint(monkeypatch):
    queries, records = [], []
    capture = worker._LivelinessEventCapture('reader', lambda event, **kw:
                                            records.append({'event': event, **kw}))
    capture.peer_sampler = lambda: queries.append(True)
    values = SimpleNamespace(alive_count=1, not_alive_count=0,
                             alive_count_change=1, not_alive_count_change=0)
    capture.callback(values)
    capture.bind('reader-gid')
    capture.close()
    capture.callback(values)
    assert queries == []
    assert len(records) == 2
    assert all(r['event'] == 'topology_liveliness_event' for r in records)


def test_worker_installs_sampler_after_real_endpoint_gid_binding():
    source = inspect.getsource(worker._worker_main)
    ast.parse(source)
    assert 'liveliness.peer_sampler = _LivelinessPeerSampler(' in source
    assert source.index('liveliness.bind(local_gid)') < source.index(
        'liveliness.peer_sampler = _LivelinessPeerSampler(')
    assert 'topology_liveliness_peer_coverage' not in source
    assert '_LivelinessPeerSampler' not in inspect.getsource(worker.OfflineTopologyBackend)


def raw_sample():
    """Pure raw contract, not an executed DDS observation."""
    identity = dict(logical_id='r', generation=1, pid=11, process_start_ticks=21,
                    endpoint_generation='rg', role='reader', run_id='run', domain_id=191,
                    monotonic_clock_identity=CLOCK)
    callback = dict(event='topology_liveliness_event', monotonic_ns=13, **identity,
                    callback_id=1, callback_monotonic_ns=12, callback_clock_identity=CLOCK,
                    observation_scope='reader_aggregate', endpoint_gid='reader-gid',
                    endpoint_gid_at_callback='reader-gid', endpoint_identity_status='bound',
                    buffered=False, during_cleanup=False, event_kind='liveliness_changed')
    native = NativeObserver().sample(191, 'reader-gid', 'reader')
    native['schema'] = worker.CycloneMatchedPeerObserver.SCHEMA
    native.update(started_monotonic_ns=14, ended_monotonic_ns=15)
    snapshot = dict(event='topology_liveliness_peer_snapshot', monotonic_ns=17,
                    **identity, schema='r2f.liveliness-peer-snapshot/v1', callback_id=1,
                    reader_gid='reader-gid', callback_monotonic_ns=12,
                    callback_clock_identity=CLOCK, sample_started_ns=14,
                    sample_finished_ns=16, sample_clock_identity=CLOCK,
                    public_count_before=1, public_count_after=1, peer_gids_complete=True,
                    matched_peer_gids=['writer-gid'], native_peer_observation=native)
    events = [dict(event='endpoint_created', monotonic_ns=1,
                   endpoint_gid='reader-gid', **identity), callback, snapshot]
    for index, row in enumerate(events):
        row['event_index'] = index
    return events


def test_independent_sample_consumer_never_promotes_continuous_coverage():
    from r2f.runtime.liveliness_evidence import evaluate_peer_samples
    events = raw_sample()
    evidence = evaluate_peer_samples(events, run_id='run', fixture=True)
    assert len(evidence) == 1
    assert evidence[0]['status'] == 'fixture_sample'
    assert evidence[0]['snapshot_ref'] == 2 and evidence[0]['callback_ref'] == 1
    assert evidence[0]['matched_peer_gids'] == ['writer-gid']
    assert evidence[0]['continuous_coverage'] is False
    assert evidence[0]['effect_observed'] is None
    assert not evidence[0]['execution_evidence'] and not evidence[0]['candidate_eligible']


@pytest.mark.parametrize('fault', ['clock', 'callback_time', 'callback_id', 'generation',
                                  'count', 'native_gid', 'raw_ref', 'run', 'missing_callback',
                                  'duplicate_callback', 'buffered', 'cleanup', 'creation'])
def test_sample_consumer_rechecks_raw_identity_and_times(fault):
    from copy import deepcopy
    from r2f.runtime.liveliness_evidence import evaluate_peer_samples
    events = deepcopy(raw_sample())
    snapshot = events[2]
    if fault == 'clock': snapshot['sample_clock_identity'] = None
    if fault == 'callback_time': snapshot['sample_started_ns'] = 11
    if fault == 'callback_id': snapshot['callback_id'] = 2
    if fault == 'generation': snapshot['generation'] = 2
    if fault == 'count': snapshot['public_count_after'] = 2
    if fault == 'native_gid': snapshot['native_peer_observation']['local_gid'] = 'other'
    if fault == 'raw_ref': snapshot['event_index'] = 99
    if fault == 'run': snapshot['run_id'] = 'other'
    if fault == 'missing_callback': events[1]['event'] = 'unrelated'
    if fault == 'duplicate_callback':
        events.insert(2, deepcopy(events[1]))
        for index, row in enumerate(events): row['event_index'] = index
    if fault == 'buffered': events[1]['buffered'] = True
    if fault == 'cleanup': events[1]['during_cleanup'] = True
    if fault == 'creation': events[0]['endpoint_gid'] = 'other'
    observation = evaluate_peer_samples(events, run_id='run', fixture=True)[0]
    assert observation['status'] == 'unknown', observation
    assert observation['effect_observed'] is None
    assert not observation['continuous_coverage']


def test_final_facts_consume_peer_samples_without_faking_ros(tmp_path):
    import json
    from pathlib import Path
    from r2f.modules.s1 import topology_model as model
    from r2f.runtime.topology_pair import run_topology

    class ScriptedBackend(worker.OfflineTopologyBackend):
        def create_endpoint(self, spec, generation, worker_role):
            handle = super().create_endpoint(spec, generation, worker_role)
            if spec['role'] == 'reader':
                # A fixture may exercise plumbing, never produce a qualified hit.
                identity = {key: handle[key] for key in
                            ('logical_id', 'generation', 'pid', 'process_start_ticks',
                             'endpoint_generation')}
                self._emit('topology_liveliness_peer_snapshot', **identity,
                           reader_gid=handle['endpoint_gid'], callback_id=1,
                           reason='fixture_missing_callback_and_native_sample')
            return handle
    program = model.generate_program(7, writers=1, readers=1)
    program['actions'] = [dict(action_id='create-r', op='create', endpoint_id='r0',
                               generation=1, params={}, depends_on=[])]
    result = run_topology(program, {'backend': 'offline_fake', 'run_id': 'peer-fixture',
                                    'domain_id': 191}, tmp_path, backend=ScriptedBackend())
    evidence = result['facts']['liveliness_peer_samples']
    assert len(evidence) == 1 and evidence[0]['status'] == 'unknown'
    assert not evidence[0]['continuous_coverage']
    assert not result['real_execution'] and not result['candidate_eligible']
    raw = [json.loads(row) for row in Path(result['raw_path']).read_text().splitlines()]
    assert raw[evidence[0]['snapshot_ref']]['event'] == 'topology_liveliness_peer_snapshot'
    assert any(row['event'] == 'topology_liveliness_peer_evidence' for row in raw)


def test_clock_discontinuity_cannot_qualify_even_a_bounded_sample(monkeypatch):
    observed = sampler(monkeypatch)
    values = iter([dict(CLOCK), {**CLOCK, 'inode': 999}])
    monkeypatch.setattr(worker, '_monotonic_clock_identity', lambda: next(values))
    result = observed()
    assert result['status'] == 'unknown' and not result['peer_gids_complete']
    assert result['reason'] == 'sample_clock_unavailable_or_changed'


def test_sampler_cannot_supply_lifecycle_or_candidate_claims():
    records = []
    capture = worker._LivelinessEventCapture('reader', lambda event, **kw:
                                            records.append({'event': event, **kw}))
    capture.bind('reader-gid')
    capture.peer_sampler = lambda: dict(status='observed_sample', continuous_coverage=True,
                                        external_peers_known=True, effect_observed=True,
                                        candidate_eligible=True, s1_mutation_hit=True,
                                        execution_evidence=True)
    capture.callback(SimpleNamespace(alive_count=1, not_alive_count=0,
                                    alive_count_change=1, not_alive_count_change=0))
    snapshot = records[-1]
    assert snapshot['event'] == 'topology_liveliness_peer_snapshot'
    assert snapshot['continuous_coverage'] is False and snapshot['external_peers_known'] is None
    assert snapshot['effect_observed'] is None
    assert not snapshot['candidate_eligible'] and not snapshot['s1_mutation_hit']
    assert not snapshot['execution_evidence']


def test_peer_snapshot_cannot_replace_trusted_continuous_coverage():
    from test_liveliness_evidence import sample
    from r2f.runtime.liveliness_evidence import evaluate_liveliness
    events, writer, reader = sample()
    proof = events[-1]
    proof.update(event='topology_liveliness_peer_snapshot', status='observed_sample',
                 peer_gids_complete=True, matched_peer_gids=[writer['endpoint_gid']])
    evidence = evaluate_liveliness(events, writer=writer, reader=reader,
                                   run_id='run', fixture=True)
    assert evidence['status'] == 'unknown'
    assert evidence['effect_observed'] is None
    assert not evidence['execution_evidence'] and not evidence['candidate_eligible']


def test_peer_sample_with_fabricated_native_time_fails_closed():
    from r2f.runtime.liveliness_evidence import evaluate_peer_samples
    events = raw_sample()
    events[-1]['native_peer_observation']['started_monotonic_ns'] = 1
    assert evaluate_peer_samples(events, run_id='run')[0]['status'] == 'unknown'


def test_duplicate_callback_sample_cannot_be_silently_chosen():
    from copy import deepcopy
    from r2f.runtime.liveliness_evidence import evaluate_peer_samples
    events = raw_sample()
    duplicate = deepcopy(events[-1])
    duplicate['event_index'] = len(events)
    events.append(duplicate)
    observations = evaluate_peer_samples(events, run_id='run')
    assert len(observations) == 2 and all(e['status'] == 'unknown' for e in observations)


def test_sample_domain_is_bound_to_creation_and_callback():
    from r2f.runtime.liveliness_evidence import evaluate_peer_samples
    events = raw_sample()
    events[-1]['domain_id'] += 1
    assert evaluate_peer_samples(events, run_id='run')[0]['reason'] == 'sample_domain_mismatch'
