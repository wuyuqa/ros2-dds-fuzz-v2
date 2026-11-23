"""P1 authored input fixtures only; no execution/qualification evidence."""
from copy import deepcopy
import pytest
from r2f.modules.s1 import topology_model as model
from test_s1_topology_model import program, action

CONTROL = {'schema': 'r2f.ros-participant-control/v1', 'mechanism': 'process_context_per_group'}

def controlled(shared=True):
    p = program()
    p['participant_control'] = dict(CONTROL)
    for i, ep in enumerate(p['endpoints']):
        ep['participant_id'] = 'group_a' if shared or i == 0 else 'group_b'
    return p

def test_producer_shaped_destroy_is_rejected_at_participant_admission():
    p = controlled()
    p['actions'].append(action('destroy_reader', 'destroy', 'r0', 1))
    result = model.validate_program(p)
    assert result['status'] == 'invalid'
    assert any(e['code'] == 'participant_control_scope' for e in result['errors']), result

def test_opt_in_binds_state_history_and_legacy_omission_is_stable():
    from r2f.runtime.topology_pair import build_input_receipts
    from test_s1_topology_model import digest
    p = controlled()
    assert model.validate_program(p)['status'] == 'valid'
    legacy = deepcopy(p); del legacy['participant_control']
    f, old = model.fingerprints(p), model.fingerprints(legacy)
    assert all(f[k] != old[k] for k in ('input_sha256','state_sha256','history_sha256'))
    assert old['input_sha256'] == digest(legacy)
    receipt = build_input_receipts(p, {'rmw':'rmw_cyclonedds_cpp'})
    assert receipt['participant_control'] == CONTROL
    assert 'participant_control' not in build_input_receipts(legacy, {})
    assert not receipt['execution_evidence'] and not receipt['candidate_eligible']

@pytest.mark.parametrize('change', ['null','tag','label','recreate','episode','filter','concurrent','multi','topic'])
def test_opt_in_rejects_cp2_and_malformed_shapes(change):
    p=controlled()
    if change == 'null': p['participant_control']=None
    elif change == 'tag': p['participant_control']['mechanism']='contexts_only'
    elif change == 'label': p['endpoints'][0]['participant_id']=None
    elif change == 'recreate': p['actions'][0]['generation']=2
    elif change == 'episode': p['episode']={}
    elif change == 'filter': p['endpoints'][1]['filter']={}
    elif change == 'concurrent': p['actions'][0]['params']['concurrency_group']='x'
    elif change == 'multi': p['endpoints'].append(deepcopy(p['endpoints'][0]))
    elif change == 'topic': p['endpoints'][1]['topic']='/another'
    assert model.validate_program(p)['status'] == 'invalid'

def test_exact_existing_plan_roundtrip_preserves_control_and_group_labels():
    from r2f.modules.s1.topology_input import make_topology_plan, validate_topology_plan
    for shared in (True,False):
        p=controlled(shared)
        plan=make_topology_plan(p,[{'operator_id':'action.insert'}],rng_seed=7)
        validate_topology_plan(plan)
        assert plan.final_config['topology_program']['participant_control']==CONTROL
        assert [e['participant_id'] for e in plan.final_config['topology_program']['endpoints']]==[
            'group_a','group_a' if shared else 'group_b']
        assert model.predict_graph(p)['pairs'][0]['status']==model.predict_graph({k:v for k,v in p.items() if k!='participant_control'})['pairs'][0]['status']

def test_legacy_model_validation_keeps_runtime_import_boundary(monkeypatch):
    import builtins
    original=builtins.__import__
    def no_runtime(name,*args,**kwargs):
        if name=='r2f.runtime' or name.startswith('r2f.runtime.'):
            raise AssertionError('legacy_model_runtime_import:'+name)
        return original(name,*args,**kwargs)
    monkeypatch.setattr(builtins,'__import__',no_runtime)
    assert model.validate_program(program())['status']=='valid'
