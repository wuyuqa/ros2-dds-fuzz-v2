"""Offline historical-window accounting, never a live ROS/DDS execution."""
from copy import deepcopy
import hashlib
import json
from pathlib import Path

import pytest

from r2f.campaign.topology_adapter import facts_from_events

LIVE = Path('/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/'
            's1-fastdds-peer-runtime-20261120/live-v4/original')
RAW_SHA = 'd477c1dacc45547b6d4c8c7efe18d311ba525cdfeab9edb15c8a0444beadc06a'


@pytest.fixture(autouse=True)
def no_native_or_ros(monkeypatch):
    def forbidden(*args, **kwargs):
        raise AssertionError('offline history accounting must not load native/ROS')
    monkeypatch.setattr('ctypes.CDLL', forbidden)
    monkeypatch.setattr('r2f.runtime.topology_worker.RosTopologyBackend.prepare', forbidden)


def live_artifacts():
    paths = list(LIVE.glob('cases/*/runtime/inner-runtime/runtime_result.json'))
    if not paths:
        pytest.skip('immutable live-v4 artifact not installed')
    runtime_path = paths[0]
    runtime = json.loads(runtime_path.read_text())
    config = json.loads((runtime_path.parents[2] / 'runtime_config.json').read_text())
    raw = Path(runtime['raw_path'])
    content = raw.read_bytes()
    assert hashlib.sha256(content).hexdigest() == RAW_SHA
    events = [json.loads(line) for line in content.splitlines()]
    return events, runtime, config, raw, content


def test_live_v4_only_the_unreceived_seq2_remains_a_historical_obligation():
    events, runtime, config, raw, content = live_artifacts()
    report_before = (LIVE / 'report.json').read_bytes()
    original = deepcopy(events)
    facts = facts_from_events(events, runtime, config)
    assert facts['diagnostic_valid'], facts['unknown_reasons']
    assert facts['unknown_reasons_sample_identity'] == []
    assert [r['event_index'] for r in facts['unresolved_historical_obligations']] == [357]
    covered = {c['send_ref']['event_index']: c for c in facts['historical_send_coverage']}
    assert set(covered) == {163, 258, 430}
    assert 357 not in covered
    assert events == original and raw.read_bytes() == content
    assert (LIVE / 'report.json').read_bytes() == report_before


def authored_history(tmp_path, mutate=None):
    """Explicit offline 1W2R fixture with overlapping reader capture windows."""
    from types import SimpleNamespace
    from test_topology_projection_occurrence import native_fixture
    from r2f.campaign.topology_sample_attribution import native_delivery_identity
    from r2f.campaign.topology_state_projection import _Projection, project_topology_state
    program, events, tokens, before, after, opened, closed, path = native_fixture(tmp_path)
    control_open = dict(opened, reader='r1')
    control_close = dict(closed, reader='r1')
    events.insert(events.index(opened) + 1, control_open)
    events.insert(events.index(closed) + 1, control_close)
    if mutate is not None:
        mutate(events, tokens, opened, closed)
    for index, event in enumerate(events):
        event['event_index'] = index
    closed['open_ref'] = opened['event_index']
    control_close['open_ref'] = control_open['event_index']
    # Additional authored closes explicitly bind the corresponding original open.
    for event in events:
        if event.pop('offline_bind_r0_open', False):
            event['open_ref'] = opened['event_index']
    content = ''.join(json.dumps(event) + '\n' for event in events).encode()
    path.write_bytes(content)
    sha = hashlib.sha256(content).hexdigest()
    projector = _Projection(program, str(path), sha, events, {'input_ref': {'event_index': 0}})
    projection = project_topology_state(program, path, before_ref=before['event_index'],
                                        after_ref=after['event_index'])
    pairs = [dict(pair, writer_generation=1, reader_generation=1)
             for pair in projection['state_after']['pairs']]
    audit = SimpleNamespace(program=program, path=str(path), sha=sha, events=events,
                            binding=events[0], tokens={(eid, 1): token for eid, token in tokens.items()},
                            specs={e['logical_id']: e for e in program['endpoints']},
                            ref=projector.ref, resolve=projector.resolve)
    audit.delivery_identity = lambda event, sends: native_delivery_identity(event, sends, events, ref=audit.ref)
    sends = [e for e in events if e['event'] == 'send']
    deliveries = [native_delivery_identity(e, sends, events, ref=audit.ref)
                  for e in events if e['event'] == 'receive']
    return audit, projection, pairs, sends, deliveries


def reconcile(authored):
    from r2f.campaign.topology_adapter import _live_history_windows, _live_history_send_coverage
    audit, projection, pairs, sends, deliveries = authored
    windows = _live_history_windows(audit, pairs, projection, deliveries)
    coverage = {s['send_sequence']: _live_history_send_coverage(audit, windows, s) for s in sends}
    return windows, coverage


def test_authored_history_requires_each_live_reader_real_native_witness(tmp_path):
    windows, coverage = reconcile(authored_history(tmp_path))
    assert all(coverage.values())
    assert len(windows) == 2 and all(w['window_projection_valid'] for w in windows)
    for sequence, proof in coverage.items():
        assert {r['reader'] for r in proof['reader_windows']} == {'r0', 'r1'}
        assert all(r['delivery_witness']['sequence'] == sequence for r in proof['reader_windows'])
        assert all(r['delivery_witness']['send_ref'] == proof['send_ref'] for r in proof['reader_windows'])


def omit_sample(sequence):
    stamp = {0: 7001, 2: 9003, 4: 12009}[sequence]
    def mutate(events, tokens, opened, closed):
        next(e for e in events if e['event'] == 'receive' and e['logical_id'] == 'r0'
             and e['message_info_receipt']['fields']['source_timestamp']['value'] == stamp)['event'] = 'offline_omitted_receive'
    return mutate


def test_closed_with_gap_covers_only_actual_samples_not_the_missing_occurrence(tmp_path):
    windows, coverage = reconcile(authored_history(tmp_path, omit_sample(2)))
    assert coverage[0] and coverage[4] and coverage[2] is None
    missing = next(w for w in windows if w['reader'] == 'r0')
    assert missing['delivery_closure'] == 'closed_with_gap'
    assert missing['proofs']['delivery_closure']['detail']['missing'] == [2]
    assert [d['sequence'] for d in missing['delivery_witnesses']] == [0, 4]
    control = next(w for w in windows if w['reader'] == 'r1')
    assert [d['sequence'] for d in control['delivery_witnesses']] == [0, 2, 4]
    assert not any(w.get('delivery_applicability') == 'not_applicable_no_sends' for w in windows)


def test_finite_tail_needs_received_final_sentinel_even_for_earlier_samples(tmp_path):
    windows, coverage = reconcile(authored_history(tmp_path, omit_sample(4)))
    assert all(c is None for c in coverage.values())
    window = next(w for w in windows if w['reader'] == 'r0')
    assert window['window_projection_valid'] and window['delivery_closure'] == 'open'


@pytest.mark.parametrize('fault', ['missing_ack', 'wrong_ack', 'incomplete_close', 'missing_close',
                                   'duplicate_close', 'overlapping_unclosed_window'])
def test_bad_or_overlapping_history_never_borrows_another_window(tmp_path, fault):
    def mutate(events, tokens, opened, closed):
        if fault == 'missing_ack':
            next(e for e in events if e['event'] == 'command_done' and e['logical_id'] == 'r0')['event'] = 'offline_missing_ack'
        elif fault == 'wrong_ack':
            next(e for e in events if e['event'] == 'command_done' and e['logical_id'] == 'r0')['command_id'] = 'wrong'
        elif fault == 'incomplete_close':
            closed['window_complete'] = False
        elif fault == 'missing_close':
            closed['event'] = 'offline_missing_close'
        elif fault == 'duplicate_close':
            events.insert(events.index(closed) + 1, dict(closed, offline_bind_r0_open=True))
        else:
            events.insert(events.index(opened) + 1, dict(opened))
    windows, coverage = reconcile(authored_history(tmp_path, mutate))
    assert all(c is None for c in coverage.values())
    rejected = [w for w in windows if not w['window_projection_valid']]
    assert rejected and all(w['reason'] and (w.get('open_ref') or w.get('close_ref')) for w in rejected)


def test_replaced_final_reader_generation_cannot_certify_still_live_history(tmp_path):
    audit, projection, pairs, sends, deliveries = authored_history(tmp_path)
    for endpoint in projection['state_after']['endpoints']:
        if endpoint['logical_id'] == 'r0':
            endpoint['generation'] = 2
    windows, coverage = reconcile((audit, projection, pairs, sends, deliveries))
    assert all(w['reader'] != 'r0' for w in windows)
    assert all(c is None for c in coverage.values())


def test_live_v4_retains_incarnations_gap_and_no_filtering_claim():
    events, runtime, config, raw, content = live_artifacts()
    facts = facts_from_events(events, runtime, config)
    windows = facts['historical_live_windows']
    assert len(windows) == 8 and all(w['window_projection_valid'] for w in windows)
    r0 = [w for w in windows if w['reader'] == 'r0']
    assert [w['reader_identity']['incarnation_ordinal'] for w in r0] == [0, 0, 1, 2]
    assert len({w['reader_identity']['endpoint_gid'] for w in r0}) == 3
    reject = next(w for w in r0 if w['delivery_closure'] == 'closed_with_gap')
    assert reject['proofs']['delivery_closure']['detail']['missing'] == [2]
    assert [d['sequence'] for d in reject['delivery_witnesses']] == [3]
    assert not facts['candidate_eligible'] and not facts['seal_eligible']
    assert raw.read_bytes() == content
