"""Explicit offline doubles at the public Cyclone entity-observer boundary."""
from test_topology_participant_identity import native, ENDPOINT, PARTICIPANT


def test_public_guid_and_iid_readbacks_bind_both_entities_without_changing_ownership(native):
    def instance(handle, pointer):
        pointer._obj.value = {10: 701, 30: 702}[handle]
        return 0
    native.lib.dds_get_instance_handle = instance
    receipt = native.sample_ownership(196, ENDPOINT, 'writer')
    assert receipt['complete'] is True
    observed = receipt['instance_identity']
    assert observed['status'] == 'observed'
    assert observed['api'] == 'dds_get_instance_handle+dds_get_guid'
    assert len(observed['samples']) == 2
    assert observed['samples'][0] == observed['samples'][1]
    assert [(r['guid'], r['instance_id']) for r in observed['samples'][0]] == [
        (PARTICIPANT, 701), (ENDPOINT, 702)]
    assert receipt['candidate_eligible'] is False


def test_missing_iid_api_keeps_ownership_but_preserves_named_unknown(native):
    receipt = native.sample_ownership(196, ENDPOINT, 'writer')
    assert receipt['complete'] is True
    observed = receipt['instance_identity']
    assert observed['status'] == 'unavailable'
    assert observed['reason'] == 'dds_get_instance_handle_unavailable'
    assert observed['samples'] == []


def test_group_initialization_event_survives_the_worker_carrier(monkeypatch, capsys, tmp_path):
    """Native probe double only; exercise the actual stdout envelope/parser."""
    import json
    import os
    from types import SimpleNamespace
    from r2f.runtime import participant_identity as probe
    from r2f.runtime.topology_worker import _EndpointProcess
    identity = probe.process_identity()
    config = dict(identity_checks={'initialization': 'fresh_process'},
        participant_control={}, group_members=[{'logical_id': 'r0', 'role': 'reader'}], group_id='g', worker_instance_id='w', context_instance_id='c',
        logical_id='r0', generation=1, run_id='offline', domain_id=80)
    monkeypatch.setattr(probe, 'run_identity_probe', lambda _: {'cleanup_complete': True})
    probe.emit_identity_probe(config, 'offline-clock')
    emitted = json.loads(capsys.readouterr().out)
    rows = []
    worker = _EndpointProcess(config, {}, tmp_path, 'group',
        lambda event, **fields: rows.append(dict(event=event, **fields)))
    worker.process = SimpleNamespace(pid=os.getpid())
    worker.metadata = identity
    # The parser requires worker PGID=PID; emulate its isolated process envelope.
    emitted['pgid'] = os.getpid()
    worker._dispatch(json.dumps(emitted).encode())
    assert [r['event'] for r in rows] == ['participant_identity_initialization']
    assert 'logical_id' not in rows[0] and 'generation' not in rows[0]
