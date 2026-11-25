"""PDP contradictions must fail common replay preflight before dispatch."""
from test_fastdds_peer_replay import bound, no_native_load, _persist_raw, project
import pytest


def test_default_blackbox_cannot_hide_native_pdp_raw(bound):
    for item in (bound.execution,bound.execution['runtime_execution_metadata'],bound.events[0]['metadata']):
        item.pop('observation_mode',None);item.pop('fastdds_peer_instrumentation',None)
    _persist_raw(bound)
    assert project(bound).get('observation_mode','blackbox')=='blackbox'
    bound.events.append({'event':'native_pdp_lifecycle','fixture':True})
    _persist_raw(bound)
    with pytest.raises(ValueError,match='pdp_trace:unselected_observer_has_raw'):project(bound)


def test_replay_rejects_pdp_selection_linked_to_foreign_peer_manifest(bound,tmp_path):
    from copy import deepcopy
    from pdp_trace_fixtures import authored_pdp, complete_rows
    from r2f.core import raw_sha256
    root=tmp_path/'pdp';root.mkdir()
    path,peer,trace=authored_pdp((bound.manifest_path,bound.manifest),root)
    reference={'manifest_path':str(path),'manifest_sha256':raw_sha256(path)}
    for item in (bound.execution,bound.execution['runtime_execution_metadata'],bound.events[0]['metadata']):
        item['fastdds_peer_instrumentation']=deepcopy(reference)
    copied=bound.raw_path.parent/'native-peer-build-manifest.json';copied.write_bytes(path.read_bytes())
    rawref=dict(observation_mode='instrumentation',manifest_path=str(copied),manifest_sha256=reference['manifest_sha256'])
    rows=complete_rows(path,peer,trace)
    for row in rows:
        row.update(run_id=bound.execution['run_id'],domain_id=bound.execution['domain_id'])
        if row['event']=='native_pdp_provider_selected':row['peer_manifest_reference']=deepcopy(rawref)
    bound.events.extend(rows);_persist_raw(bound)
    assert project(bound)['observation_mode']=='instrumentation'
    chosen=next(e for e in rows if e['event']=='native_pdp_provider_selected')
    chosen['peer_manifest_reference']['manifest_path']='/foreign/native-peer-build-manifest.json'
    _persist_raw(bound)
    with pytest.raises(ValueError,match='pdp_trace:worker_selection_mismatch'):project(bound)
