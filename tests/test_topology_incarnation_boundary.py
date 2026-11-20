"""File-only worker boundary identity checks, never kernel attestation."""
from copy import deepcopy
import pytest
from test_endpoint_incarnation import trace,OLD,NEW
from r2f.campaign.boundary_audit import _incarnation_process_exits

KEYS=('logical_id','generation','pid','process_start_ticks','endpoint_generation','endpoint_gid')


def exit_trace():
    rows=trace()
    rows.append(dict(rows[0],event='process_exit',endpoint_gid=NEW,
                     event_index=len(rows),monotonic_ns=108,returncode=0))
    return rows


def test_changed_exit_matches_authenticated_current_not_binding_origin():
    rows=exit_trace();worker={k:rows[0][k] for k in KEYS}
    assert _incarnation_process_exits(rows,worker,KEYS)==[rows[-1]]
    assert worker['endpoint_gid']==OLD


@pytest.mark.parametrize('fault',['old_exit','missing_transition','wrong_ack','old_clock','different_worker','wrong_binding'])
def test_boundary_exit_never_waives_identity_proof(fault):
    rows=exit_trace();worker={k:rows[0][k] for k in KEYS}
    if fault=='old_exit': rows[-1]['endpoint_gid']=OLD
    elif fault=='missing_transition': rows=[e for e in rows if e['event']!='endpoint_identity_transition']
    elif fault=='wrong_ack':rows[-2]['reader_gid']=OLD
    elif fault=='old_clock':rows[-1]['monotonic_ns']=103
    elif fault=='different_worker':rows[-1]['endpoint_generation']='wrong'
    else:worker['endpoint_gid']=NEW
    with pytest.raises(ValueError):_incarnation_process_exits(rows,worker,KEYS)


def test_legacy_no_transition_exit_still_matches_exact_origin():
    rows=trace()[:1]
    rows.append(dict(rows[0],event='process_exit',event_index=1,monotonic_ns=101))
    worker={k:rows[0][k] for k in KEYS}
    assert _incarnation_process_exits(rows,worker,KEYS)==[rows[-1]]


def test_boundary_does_not_accept_a_transition_after_exit():
    rows=exit_trace()
    early=rows.pop()
    early.update(endpoint_gid=OLD,monotonic_ns=101)
    rows.insert(1,early)
    for i,row in enumerate(rows):row['event_index']=i
    worker={k:rows[0][k] for k in KEYS}
    with pytest.raises(ValueError,match='process_identity_transition_after_exit'):
        _incarnation_process_exits(rows,worker,KEYS)
