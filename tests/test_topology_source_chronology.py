"""Source phase qualification; measurement is explicitly stubbed, never DDS."""
from copy import deepcopy
from types import SimpleNamespace

import pytest

from r2f.campaign.topology_adapter import _source_qualification

SOURCE = {'source_frozen': True, 'source_repo': '/offline/source-measurement-stub', 'head': 'a' * 40}


def audit(monkeypatch):
    monkeypatch.setattr('r2f.campaign.identity.source_manifest', lambda repo: deepcopy(SOURCE))
    events = []
    def add(kind, **fields):
        events.append(dict(event=kind, event_index=len(events), monotonic_ns=len(events) + 1, **fields))
    def check(name):
        add('topology_source_checked', phase=name, source=deepcopy(SOURCE))
    check('before_preflight'); check('after_preflight'); check('before_presteps')
    add('state_step_before', step_id='admit_topology')
    # Concurrently nested begins are valid; the second writer is not backfilled.
    check('before_action:w0'); add('topology_action_begin', action_id='w0')
    check('before_action:w1'); add('topology_action_begin', action_id='w1')
    add('topology_action_done', action_id='w0'); add('topology_action_done', action_id='w1')
    check('after_actions'); add('topology_cleanup'); check('after_cleanup')
    return SimpleNamespace(fixture=False, config={'source_manifest': deepcopy(SOURCE)}, events=events)


def reindex(a):
    for i, e in enumerate(a.events):
        e.update(event_index=i, monotonic_ns=i+1)


def test_nested_concurrent_source_checks_and_prefix_qualify(monkeypatch):
    a = audit(monkeypatch)
    assert _source_qualification(a)
    assert _source_qualification(a, before=a.events[3])


@pytest.mark.parametrize('phase', ['before_preflight', 'after_preflight', 'before_presteps',
    'before_action:w0', 'before_action:w1', 'after_actions', 'after_cleanup'])
def test_missing_phase_never_borrows_source_equal_bookends(monkeypatch, phase):
    a = audit(monkeypatch)
    a.events = [e for e in a.events if e.get('phase') != phase]
    reindex(a)
    assert not _source_qualification(a)


@pytest.mark.parametrize('phase', ['before_preflight', 'before_action:w0', 'before_action:w1', 'after_cleanup'])
def test_duplicate_phase_rejected(monkeypatch, phase):
    a = audit(monkeypatch)
    a.events.insert(-1, deepcopy(next(e for e in a.events if e.get('phase') == phase)))
    reindex(a)
    assert not _source_qualification(a)


@pytest.mark.parametrize('phase,target', [('before_action:w1', 'topology_action_done'),
    ('after_actions', 'topology_action_begin'), ('after_cleanup', 'topology_cleanup')])
def test_wrong_raw_phase_order_rejected_even_when_source_matches(monkeypatch, phase, target):
    a = audit(monkeypatch)
    gate = next(e for e in a.events if e.get('phase') == phase)
    a.events.remove(gate)
    at = next(i for i, e in enumerate(a.events) if e['event'] == target)
    a.events.insert(at + (phase == 'before_action:w1'), gate)
    reindex(a)
    assert not _source_qualification(a)


def test_cleanup_must_follow_last_done_and_admission_cannot_use_future_source(monkeypatch):
    a = audit(monkeypatch)
    cleanup = next(e for e in a.events if e['event'] == 'topology_cleanup')
    a.events.remove(cleanup); a.events.insert(7, cleanup); reindex(a)
    assert not _source_qualification(a)
    a = audit(monkeypatch)
    admission = a.events[3]
    gate = next(e for e in a.events if e.get('phase') == 'after_preflight')
    a.events.remove(gate); a.events.append(gate); reindex(a)
    assert not _source_qualification(a, before=admission)


def test_source_drift_and_fixture_remain_unqualified(monkeypatch):
    a = audit(monkeypatch)
    a.fixture = True
    assert not _source_qualification(a)
    a.fixture = False
    monkeypatch.setattr('r2f.campaign.identity.source_manifest', lambda repo: dict(SOURCE, head='b'*40))
    assert not _source_qualification(a)
