"""L03 raw ordering observations. All fixtures are explicitly non-DDS/non-real.

No new target rule/residual is declared, and none of these observations confer
candidate eligibility. Arrival means persisted reader callback stream order.
"""
from copy import deepcopy

import pytest

from test_topology_adapter import RawFixture
from r2f.campaign.topology_adapter import facts_from_events
from r2f.modules.s1.topology_oracles import evaluate_topology
from r2f.runtime.topology_pair import pair_delivery_fact


@pytest.fixture(autouse=True)
def forbid_live_ros(monkeypatch):
    def blocked(*args, **kwargs):
        raise AssertionError("L03 tests are offline only; ROS backend forbidden")
    monkeypatch.setattr('r2f.runtime.topology_worker.RosTopologyBackend.prepare', blocked)


def receive_order(fixture, order):
    """Author receive callbacks in the requested order; preserve capture clocks."""
    rows = [e for e in fixture.events if e['event'] == 'receive']
    originals = {(e['payload_writer_id'], e['send_sequence']): deepcopy(e) for e in rows}
    assert len(rows) == len(order)
    for slot, key in zip(rows, order):
        clocks = {k: slot[k] for k in ('event_index', 'monotonic_ns', 'time_ns')}
        slot.clear()
        slot.update(deepcopy(originals[key]), **clocks)


def observation(facts, writer='w0'):
    return next(p['ordering_observation'] for p in facts['pairs'] if p['writer'] == writer)


def renumber(fixture):
    indices = {e['event_index']: i for i, e in enumerate(fixture.events)}
    for i, e in enumerate(fixture.events):
        if e['event'] == 'topology_delivery_window_closed':
            e['open_ref'] = indices[e['open_ref']]
        e.update(event_index=i, monotonic_ns=1000 + i * 1000,
                 time_ns=1700000000000000000 + i * 1000)


def test_raw_single_writer_inversion_not_hidden_by_equal_sequence_set(tmp_path):
    f = RawFixture()
    receive_order(f, [('w0', 2), ('w0', 1), ('w0', 3)])
    facts = f.facts(tmp_path)
    assert facts['diagnostic_valid'], facts.get('evidence_error')
    obs = observation(facts)
    assert obs['send_order'] == [1, 2, 3]
    assert obs['receive_order'] == [2, 1, 3]
    assert obs['status'] == 'inversion_observed'
    assert obs['inversions'][0]['sequences'] == [2, 1]
    assert obs['writer_gid'] == 'gid-w0' and obs['reader_gid'] == 'gid-r0'
    assert obs['writer_generation'] == obs['reader_generation'] == 1
    assert all(ref['sha256'] == facts['raw']['sha256'] for ref in obs['receive_refs'])
    report = evaluate_topology(f.scenario, facts)
    assert report['ordering_observations'] == [obs]
    assert report['ordering_oracle_coverage'] == 'not_declared_by_target_rule'
    assert report['verdict'] == 'unknown'  # legacy gate preserved, no invented rule
    assert not report['forbidden_residuals'] and not report['candidate_eligible']
    assert obs['rule_verdict'] is None and not obs['candidate_eligible']


@pytest.mark.parametrize('inverted', [False, True])
def test_two_writers_interleave_without_total_order_requirement(tmp_path, inverted):
    f = RawFixture(writers=2)
    # Authored overlapping windows; not a claim of live concurrent execution.
    first_close = next(e for e in f.events if e['event'] == 'topology_delivery_window_closed')
    f.events.remove(first_close)
    last_close = max(i for i, e in enumerate(f.events) if e['event'] == 'topology_delivery_window_closed')
    f.events.insert(last_close + 1, first_close)
    # Put the second pair's fresh snapshots/open before either callback stream.
    second_open = next(e for e in f.events if e['event'] == 'topology_delivery_window_open'
                       and e['writer'] == 'w1')
    end = f.events.index(second_open)
    start = end - 4 * len(f.tokens)
    block = f.events[start:end + 1]
    del f.events[start:end + 1]
    first_open = next(i for i, e in enumerate(f.events) if e['event'] == 'topology_delivery_window_open')
    f.events[first_open:first_open] = block
    renumber(f)
    first, second = (2, 1) if inverted else (1, 2)
    receive_order(f, [('w0', first), ('w1', 1), ('w1', 2), ('w0', second), ('w0', 3), ('w1', 3)])
    facts = f.facts(tmp_path)
    assert facts['diagnostic_valid'], facts.get('evidence_error')
    for writer in ('w0', 'w1'):
        obs = observation(facts, writer)
        bad = inverted and writer == 'w0'
        assert obs['status'] == ('inversion_observed' if bad else 'in_order_observed'), obs
        assert obs['receive_order'] == ([2, 1, 3] if bad else [1, 2, 3])
        assert obs['cross_writer_order_required'] is False
        assert bool(obs['inversions']) == bad


@pytest.mark.parametrize('missing', [(2,), (3,)])
def test_middle_missing_is_not_order_inversion_and_tail_is_unknown(tmp_path, missing):
    f = RawFixture(missing=missing)
    facts = f.facts(tmp_path)
    obs = observation(facts)
    assert obs['status'] == ('unknown' if missing == (3,) else 'in_order_observed')
    assert obs['inversions'] == []
    if missing == (3,):
        report = evaluate_topology(f.scenario, facts)
        assert report['verdict'] == 'unknown' and not report['forbidden_residuals']


@pytest.mark.parametrize('mode', ['duplicate', 'incomplete', 'no_close', 'late_tail'])
def test_ambiguous_or_unclosed_never_judged_ordered(tmp_path, mode):
    f = RawFixture()
    if mode == 'duplicate':
        receive_order(f, [('w0', 1), ('w0', 1), ('w0', 3)])
    elif mode == 'no_close':
        f.events[:] = [e for e in f.events if e['event'] != 'topology_delivery_window_closed']
        renumber(f)
    elif mode == 'late_tail':
        tail = next(e for e in f.events if e['event'] == 'receive' and e['send_sequence'] == 3)
        f.events.remove(tail)
        i = next(i for i, e in enumerate(f.events) if e['event'] == 'topology_delivery_window_closed')
        f.events.insert(i + 1, tail)
        renumber(f)
    else:
        next(e for e in f.events if e['event'] == 'topology_delivery_window_closed')['window_complete'] = False
    facts = f.facts(tmp_path)
    assert facts['diagnostic_valid'], facts.get('evidence_error')
    assert observation(facts)['status'] == 'unknown'
    assert not facts['candidate_eligible']


@pytest.mark.parametrize('field,value', [('reader_gid', 'foreign'), ('publisher_gid', 'foreign'),
                                        ('generation', 2), ('endpoint_generation', 'new-worker')])
def test_foreign_identity_or_generation_never_merged(tmp_path, field, value):
    f = RawFixture()
    next(e for e in f.events if e['event'] == 'receive')[field] = value
    facts = f.facts(tmp_path)
    assert facts.get('evidence_error') or observation(facts)['status'] == 'unknown'
    assert not facts['candidate_eligible']
    assert not any(p.get('ordering_observation', {}).get('status') == 'in_order_observed'
                   for p in facts['pairs'])


def test_missing_gid_remains_endpoint_correlated_not_direct(tmp_path):
    f = RawFixture(publisher_gid=False)
    facts = f.facts(tmp_path)
    obs = observation(facts)
    assert obs['status'] == 'in_order_observed'
    assert obs['identity_bases'] == ['endpoint_correlated_payload']
    assert all(not w['direct_gid_match'] and w['publisher_gid'] is None
               and w['publisher_gid_status'] == 'unavailable_api' for w in obs['witnesses'])


def test_payload_ambiguity_fails_closed(tmp_path):
    f = RawFixture(publisher_gid=False)
    receives = [e for e in f.events if e['event'] == 'receive']
    receives[1]['payload_sha256'] = receives[0]['payload_sha256']
    facts = f.facts(tmp_path)
    assert facts.get('evidence_error') or observation(facts)['status'] == 'unknown'
    assert not facts['candidate_eligible']


def test_raw_hash_tamper_and_helper_summary_do_not_supply_order(tmp_path):
    f = RawFixture().save(tmp_path)
    f.runtime['facts']['ordering_observation'] = {'status': 'inversion_observed'}
    facts = facts_from_events(f.events, f.runtime, f.config)
    assert observation(facts)['status'] == 'in_order_observed'
    f.raw.write_bytes(f.raw.read_bytes() + b'\n')
    bad = facts_from_events(f.events, f.runtime, f.config)
    assert bad['evidence_error'] == 'raw_sha256_mismatch'
    assert not bad['candidate_eligible'] and bad['pairs'] == []


def test_producer_retains_mixed_attribution_arrival_order_not_category_order():
    f = RawFixture()
    receive_order(f, [('w0', 2), ('w0', 1), ('w0', 3)])
    received = [e for e in f.events if e['event'] == 'receive']
    received[1].update(publisher_gid=None, publisher_gid_status='unavailable_api',
                       payload_endpoint_gid='gid-w0')
    pair = dict(writer='w0', reader='r0', writer_generation=1, reader_generation=1,
                writer_gid='gid-w0', reader_gid='gid-r0', status='matched')
    fact = pair_delivery_fact(pair, f.events)
    assert fact['receive_order'] == [2, 1, 3]
    assert fact['receive_order_refs'] == [e['event_index'] for e in received]
    assert fact['in_order'] is False
    assert fact['correlated_receipts'] == 1
    assert not fact['candidate_eligible']


def test_retired_writer_receives_are_not_new_generation_order(tmp_path):
    from test_topology_recreated_history_accounting import produce_history
    runtime, config = produce_history(tmp_path)
    facts = facts_from_events(runtime['events'], runtime, config)
    assert facts['diagnostic_valid'], facts.get('evidence_error')
    assert any(d['writer'] == 'w0' and d['writer_generation'] == 1 for d in facts['deliveries'])
    current = [p for p in facts['pairs'] if p['writer'] == 'w0']
    assert current and all(p['writer_generation'] == 2 for p in current)
    for pair in current:
        obs = pair['ordering_observation']
        assert obs['writer_generation'] == 2 and obs['status'] == 'unknown'
        assert not obs.get('receive_order')
    assert not facts['candidate_eligible']


def test_best_effort_or_insufficient_observed_history_cannot_grant_order(tmp_path):
    f = RawFixture(depth=1)
    facts = f.facts(tmp_path)
    assert facts['diagnostic_valid']
    obs = observation(facts)
    assert obs['receive_order'] == [1, 2, 3]
    assert obs['status'] == 'unknown'
    assert obs['reason_codes'] == ['reliable_unfiltered_contract_unproven']
