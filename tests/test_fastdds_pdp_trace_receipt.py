"""Common execution audit rejects selected PDP observers without raw receipts."""
import pytest
from test_fastdds_peer_probe import build_manifest
from test_fastdds_peer_activation import config
from pdp_trace_fixtures import authored_pdp, worker_rows
from r2f.instrumentation.fastdds_peer.receipt import validate_execution_mode


def test_selected_pdp_requires_worker_selection_and_final_health(build_manifest,tmp_path):
    path,manifest,_=authored_pdp(build_manifest,tmp_path)
    with pytest.raises(ValueError,match='pdp_trace'):
        validate_execution_mode(worker_rows(path,manifest),config(path))


@pytest.mark.parametrize('enabled',[True,False])
def test_pdp_receipts_use_same_common_execution_audit(build_manifest,tmp_path,enabled):
    from pdp_trace_fixtures import complete_rows
    path,manifest,trace=authored_pdp(build_manifest,tmp_path,enabled=enabled)
    assert validate_execution_mode(complete_rows(path,manifest,trace),config(path))


@pytest.mark.parametrize('damage',['unbound_worker','missing_final','loss','clock','process','ordinal','call','mapping','unexpected_event','selection','disabled_capture'])
def test_pdp_raw_contradictions_cannot_qualify(build_manifest,tmp_path,damage):
    from pdp_trace_fixtures import complete_rows
    path,manifest,trace=authored_pdp(build_manifest,tmp_path)
    rows=complete_rows(path,manifest,trace)
    if damage=='unbound_worker':rows[3]['pid']=999
    elif damage=='missing_final':rows.pop()
    elif damage=='loss':rows[-1]['stats']['dropped']=1
    elif damage=='clock':rows[3]['receipt']['capture_monotonic_ns']=111
    elif damage=='process':rows[3]['receipt']['process_pid']=999
    elif damage=='ordinal':rows[3]['receipt']['ordinal']=2
    elif damage=='call':rows[4]['receipt']['call_id']=2
    elif damage=='mapping':rows[1]['provider_mapping']['inode']+=1
    elif damage=='unexpected_event':rows.append(dict(rows[1],event='native_pdp_trace_close_error'))
    elif damage=='selection':rows[1]['capture_enabled']=False
    else:
        trace['capture_enabled']=False
        import json
        from pathlib import Path
        from r2f.instrumentation.fastdds_pdp_trace.activation import digest
        t=Path(manifest['pdp_trace']['manifest_path']);t.write_text(json.dumps(trace))
        manifest['pdp_trace']['manifest_sha256']=digest(t);path.write_text(json.dumps(manifest))
        rows=complete_rows(path,manifest,dict(trace,capture_enabled=True));rows[1]['capture_enabled']=False
        rows[-1]['stats']['enabled']=0
    with pytest.raises(ValueError,match='pdp_trace'):
        validate_execution_mode(rows,config(path))


def test_blackbox_rejects_unselected_pdp_raw():
    with pytest.raises(ValueError,match='unselected_observer_has_raw'):
        validate_execution_mode([{'event':'native_pdp_lifecycle'}],{})


def test_final_health_cannot_reference_another_observer(build_manifest,tmp_path):
    from pdp_trace_fixtures import complete_rows
    path,manifest,trace=authored_pdp(build_manifest,tmp_path)
    rows=complete_rows(path,manifest,trace)
    rows[-1]['manifest_reference']={'manifest_path':'/foreign/trace.json','manifest_sha256':'0'*64}
    with pytest.raises(ValueError,match='pdp_trace'):
        validate_execution_mode(rows,config(path))


def test_final_health_before_context_shutdown_is_incomplete(build_manifest,tmp_path):
    from pdp_trace_fixtures import complete_rows
    path,manifest,trace=authored_pdp(build_manifest,tmp_path)
    rows=complete_rows(path,manifest,trace)
    rows[-1]['event_index']=5
    rows[-1]['monotonic_ns']=150
    with pytest.raises(ValueError,match='pdp_trace'):
        validate_execution_mode(rows,config(path))
