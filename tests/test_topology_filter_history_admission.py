"""Offline consumers of immutable actual-capture artifacts; never a new DDS run."""
from copy import deepcopy
import hashlib
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from r2f.campaign.topology_adapter import facts_from_events
from r2f.modules.s1.topology_oracles import evaluate_topology
from r2f.modules.s1.topology_input import TOPOLOGY_TARGET

LIVE = Path('/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/'
            's1-fastdds-peer-runtime-20261120/live-v5/original')
RAW_SHA = 'dd5579568dce85c3efe6003685804c981cd225f4668469e64423f91291ce56ba'


@pytest.fixture(autouse=True)
def no_ros_or_native(monkeypatch):
    def forbidden(*args, **kwargs):
        raise AssertionError('receipt consumption must not start ROS or load native code')
    monkeypatch.setattr('ctypes.CDLL', forbidden)
    monkeypatch.setattr('r2f.runtime.topology_worker.RosTopologyBackend.prepare', forbidden)


def actual_artifacts():
    paths = list(LIVE.glob('cases/*/runtime/inner-runtime/runtime_result.json'))
    if not paths:
        pytest.skip('immutable live-v5 artifact not installed')
    p=paths[0];runtime=json.loads(p.read_text())
    config=json.loads((p.parents[2]/'runtime_config.json').read_text())
    raw=Path(runtime['raw_path']);content=raw.read_bytes()
    assert hashlib.sha256(content).hexdigest()==RAW_SHA
    return [json.loads(line) for line in content.splitlines()],runtime,config,raw,content


def test_actual_false_and_positive_sentinel_discharge_only_exact_historical_obligation():
    events,runtime,config,raw,content=actual_artifacts()
    old_events=deepcopy(events);report=(LIVE/'report.json').read_bytes()
    facts=facts_from_events(events,runtime,config)
    assert facts['diagnostic_valid'],facts['unknown_reasons']
    assert facts['unknown_reasons_sample_identity']==[]
    assert facts['unresolved_historical_obligations']==[]
    coverage=next(c for c in facts['historical_send_coverage'] if c['send_ref']['event_index']==395)
    reader=next(w for w in coverage['reader_windows'] if w['reader']=='r0')
    assert reader['delivery_disposition']=='filtered_by_actual_native_decision'
    proof=reader['filter_decision_evidence']
    assert proof['filter_effect_observed'] is True and proof['candidate_eligible'] is False
    assert proof['rejected']['sequence']==2 and proof['rejected']['decision'] is False
    assert proof['accepted_sentinel']['sequence']==3 and proof['accepted_sentinel']['decision'] is True
    assert proof['rejected']['decision_ref']['event_index']==396
    assert proof['accepted_sentinel']['decision_ref']['event_index']==479
    # It is still a real missing delivery, not a rewritten receive/window.
    gap=next(w for w in facts['historical_live_windows'] if w['reader']=='r0' and w['delivery_closure']=='closed_with_gap')
    assert gap['proofs']['delivery_closure']['detail']['missing']==[2]
    assert not any(d['reader']=='r0' and d['sequence']==2 for d in facts['deliveries'])
    verdict=evaluate_topology(SimpleNamespace(target_id=TOPOLOGY_TARGET,arm=config['arm']),facts)
    assert verdict['verdict']=='pass',verdict['reason_codes']
    assert verdict['candidate_eligible'] is False and facts['seal_eligible'] is False
    assert events==old_events and raw.read_bytes()==content and (LIVE/'report.json').read_bytes()==report


def consumer_fixture(tmp_path):
    # Authored already-validated decision objects test ONLY the consumer seam.
    # The helper's native/raw provenance is separately required by the full path.
    from test_topology_live_history_windows import authored_history, omit_sample, reconcile
    authored=authored_history(tmp_path,omit_sample(2))
    audit,_,_,sends,_=authored
    windows,_=reconcile(authored)
    target=next(w for w in windows if w['reader']=='r0')
    rejected_send=next(s for s in sends if s['send_sequence']==2)
    sentinel=next(s for s in sends if s['send_sequence']==4)
    common=dict(writer=target['writer'],writer_generation=target['writer_generation'],
        writer_gid=target['writer_identity']['endpoint_gid'],reader='r0',reader_generation=1,
        reader_gid=target['reader_identity']['endpoint_gid'],
        reader_incarnation=target['reader_identity']['incarnation_ordinal'],
        action_id='authored-enabled-action',action_result_ref={'authored':'result'},
        state_ref={'authored':'state'},filter_effect_observed=None,candidate_eligible=False)
    observations=[dict(common,sequence=s['send_sequence'],decision=decision,send_ref=audit.ref(s),
        decision_ref=audit.ref(s),capture_started_ns=s['monotonic_ns'],capture_finished_ns=s['monotonic_ns'])
        for s,decision in ((rejected_send,False),(sentinel,True))]
    return audit,windows,rejected_send,observations


def test_exact_native_reject_is_not_invented_receive_and_does_not_skip_other_reader(tmp_path):
    from r2f.campaign.topology_adapter import _live_history_send_coverage
    audit,windows,send,observations=consumer_fixture(tmp_path)
    assert _live_history_send_coverage(audit,windows,send) is None
    proof=_live_history_send_coverage(audit,windows,send,filter_decisions=observations)
    assert proof is not None
    rows={r['reader']:r for r in proof['reader_windows']}
    assert rows['r0']['delivery_witness'] is None
    assert rows['r0']['filter_decision_evidence']['filter_effect_observed'] is True
    assert rows['r1']['delivery_witness']['sequence']==2 and rows['r1']['delivery_disposition']=='received'
    assert next(w for w in windows if w['reader']=='r0')['proofs']['delivery_closure']['detail']['missing']==[2]


@pytest.mark.parametrize('damage', ['no_actual_false','no_true_sentinel','different_send','wrong_reader_gid',
    'wrong_incarnation','wrong_generation','other_action','other_state','capture_outside',
    'duplicate_false','late_sentinel_capture','finite_tail','missing_control_delivery'])
def test_native_rejection_cannot_waive_other_obligations(tmp_path,damage):
    from r2f.campaign.topology_adapter import _live_history_send_coverage
    audit,windows,send,observations=consumer_fixture(tmp_path)
    row=next(w for w in windows if w['reader']=='r0')
    if damage=='no_actual_false':observations[0]['decision']=True
    elif damage=='no_true_sentinel':observations.pop()
    elif damage=='different_send':observations[0]['send_ref']=observations[1]['send_ref']
    elif damage=='wrong_reader_gid':observations[0]['reader_gid']='ff'*16
    elif damage=='wrong_incarnation':observations[0]['reader_incarnation']+=1
    elif damage=='wrong_generation':observations[0]['reader_generation']+=1
    elif damage=='other_action':observations[1]['action_id']='other'
    elif damage=='other_state':observations[1]['state_ref']={'authored':'other-state'}
    elif damage=='capture_outside':observations[0]['capture_started_ns']=0
    elif damage=='duplicate_false':observations.append(deepcopy(observations[0]))
    elif damage=='late_sentinel_capture':
        observations[1]['capture_finished_ns']=audit.resolve(row['close_ref'])['monotonic_ns']
    elif damage=='finite_tail':row['delivery_closure']='open'
    else:next(w for w in windows if w['reader']=='r1')['delivery_witnesses']=[]
    assert _live_history_send_coverage(audit,windows,send,filter_decisions=observations) is None


@pytest.mark.parametrize('damage', ['drop_reject','drop_accept','unknown_worker'])
def test_full_adapter_does_not_treat_partial_declared_capture_as_a_waiver(monkeypatch,damage):
    import r2f.campaign.topology_filter_decision as helper
    original=helper.native_filter_decision_evidence
    def partial(*args,**kwargs):
        value=original(*args,**kwargs)
        if damage=='unknown_worker':value['unknowns'].append(dict(reason='authored unavailable worker proof'))
        else:value['observations']=[o for o in value['observations'] if o['decision'] is (damage=='drop_reject')]
        return value
    monkeypatch.setattr(helper,'native_filter_decision_evidence',partial)
    events,runtime,config,_,_=actual_artifacts()
    facts=facts_from_events(events,runtime,config)
    assert facts['diagnostic_valid']
    assert [r['event_index'] for r in facts['unresolved_historical_obligations']]==[395]
    assert facts['dynamic_filter_decision_effects']==[]
    verdict=evaluate_topology(SimpleNamespace(target_id=TOPOLOGY_TARGET,arm=config['arm']),facts)
    assert verdict['verdict']=='unknown' and not verdict['candidate_eligible']
