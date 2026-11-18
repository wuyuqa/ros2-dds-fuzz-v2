"""Offline scope memo contract: no hook/generation/source gate exemptions."""
from copy import deepcopy
import json
import os
from pathlib import Path
import subprocess
import sys
from unittest.mock import patch

import pytest
import test_model_search_topology as t
from r2f.campaign.search_frontier import (InputSearchFrontier, _frontier_validation_scope,
                                         _VALIDATION_MEMO, _memo_key)
from r2f.campaign.checkpoint import _identity, _validate_state
from r2f.core.evidence import canonical_hash


def test_exact_structural_reuse_is_detached_and_scope_local():
    doc=t.narrowed();state=t.fresh(doc);t.generate_one(state,doc)
    raw=state['scheduler'].frontier.to_dict()
    with patch.object(InputSearchFrontier,'prefix',wraps=InputSearchFrontier.prefix) as replay:
        with _frontier_validation_scope():
            a=InputSearchFrontier.from_dict(raw)
            b=InputSearchFrontier.from_dict(deepcopy(raw))
            assert a.to_dict()==b.to_dict()==raw
            assert replay.call_count==1
            a.tasks[0]['status']='forged'
            b.nodes.clear()
            assert InputSearchFrontier.from_dict(raw).to_dict()==raw
            forged=deepcopy(raw);forged['tasks'][0]['status']='forged'
            with pytest.raises(ValueError):
                InputSearchFrontier.from_dict(forged)
        assert _VALIDATION_MEMO.get() is None
        InputSearchFrontier.from_dict(raw)
        assert replay.call_count==3


def test_scope_failure_and_nested_validation_do_not_leak():
    assert _VALIDATION_MEMO.get() is None
    with pytest.raises(RuntimeError):
        with _frontier_validation_scope():
            outer=_VALIDATION_MEMO.get();outer['noop']['sentinel']=False
            with _frontier_validation_scope():
                assert _VALIDATION_MEMO.get() is not outer
                assert not _VALIDATION_MEMO.get()['noop']
            assert _VALIDATION_MEMO.get() is outer
            raise RuntimeError('failure')
    assert _VALIDATION_MEMO.get() is None


@pytest.mark.parametrize('value',[{1:'x'}, {'tuple':(1,2)}, {'object':object()}])
def test_non_json_memo_keys_decline_without_normalizing(value):
    assert _memo_key(value) is None


def test_noop_cache_revalidates_changed_full_plan_not_supplied_hash():
    doc=t.narrowed();state=t.fresh(doc);_,record=t.generate_one(state,doc)
    raw=record['mutation_plan']
    from r2f.core.mutation_plan import MutationPlan
    with patch.object(MutationPlan,'from_dict',wraps=MutationPlan.from_dict) as parse:
        with _frontier_validation_scope():
            expected=InputSearchFrontier._noop_plan(raw)
            assert InputSearchFrontier._noop_plan(deepcopy(raw))==expected
            assert parse.call_count==1
            forged=deepcopy(raw);forged['unexpected_field']='not covered by a supplied hash'
            with pytest.raises(ValueError):
                InputSearchFrontier._noop_plan(forged)
        InputSearchFrontier._noop_plan(raw)
        assert parse.call_count==3


def test_standard_hook_cold_model_validation_and_rehashed_rejection_tamper(tmp_path):
    doc=t.narrowed('relation.pair','S1.topology.qos.reliability',{'pair':'BB'})
    t.run(tmp_path/'old',doc,maximum=1)
    path=tmp_path/'old/checkpoint.json';data=json.loads(path.read_text())
    original=deepcopy(data['state'])
    identity,seeds,registry=_identity(doc,t.CYCLE,t.SOURCE)
    with patch.object(InputSearchFrontier,'validate_model',autospec=True,
                      side_effect=InputSearchFrontier.validate_model) as model:
        _,_,scheduler,_=_validate_state(original,identity,seeds,registry)
        assert model.call_count==1
    assert original==data['state']
    assert scheduler.to_dict()==original['scheduler']
    # Rebuild a structurally coherent lie, then rehash every outer scheduler and
    # checkpoint envelope. The authoritative generator must still reject it.
    raw=deepcopy(data['state']['scheduler']['frontier'])
    rebuilt=InputSearchFrontier.prefix(raw,0)
    for event in deepcopy(raw['journal']):
        if event['action']=='generation_rejected':event['reason']='forged rejection'
        if event['action']=='finish':event['reason']='topology_input_rejected:forged rejection'
        rebuilt.apply_event(event)
    data['state']['scheduler']['frontier']=rebuilt.to_dict()
    state=data['state']['scheduler']
    state['selection_identity']=canonical_hash({k:v for k,v in state.items() if k!='selection_identity'})
    data['checkpoint_sha256']=canonical_hash({k:v for k,v in data.items() if k!='checkpoint_sha256'})
    path.write_text(json.dumps(data))
    with _frontier_validation_scope():
        InputSearchFrontier.from_dict(original['scheduler']['frontier'])
        # Even in the same parent scope, the re-signed changed journal must
        # reconstruct its own state, not receive the formerly valid frontier.
        changed=InputSearchFrontier.from_dict(state['frontier'])
        assert changed.to_dict()!=original['scheduler']['frontier']
        with pytest.raises(ValueError,match='rejection not model-generatable'):
            changed.validate_model(profile=doc,seeds=seeds,registry=registry)
        with pytest.raises(ValueError,match='rejection not model-generatable'):
            t.restore(path,tmp_path/'refused',doc)
    assert not (tmp_path/'refused').exists()


def test_fresh_process_restore_regenerates_every_plan(tmp_path):
    doc=t.narrowed();doc['search']['max_depth']=2
    t.run(tmp_path/'old',doc,maximum=2)
    (tmp_path/'profile.json').write_text(json.dumps(doc))
    code='''
import json,sys
from pathlib import Path
from unittest.mock import patch
import test_model_search_topology as t
from r2f.campaign import topology_search
from r2f.campaign.search_frontier import _VALIDATION_MEMO
p=Path(sys.argv[1]);doc=json.loads((p/'profile.json').read_text())
raw=json.loads((p/'old/checkpoint.json').read_text())
assert _VALIDATION_MEMO.get() is None
expected=sum(e['action'] in {'plan','generation_rejected'} for e in raw['state']['scheduler']['frontier']['journal'])
with patch.object(topology_search,'generate',wraps=topology_search.generate) as generate:
    state=t.restore(p/'old/checkpoint.json',p/'new',doc)
    assert generate.call_count==expected
assert _VALIDATION_MEMO.get() is None
assert state['scheduler'].to_dict()==raw['state']['scheduler']
print(json.dumps({'regenerations':expected,'cold_process':True}))
'''
    root=Path(__file__).resolve().parents[1]
    env=dict(os.environ,PYTHONPATH=os.pathsep.join([str(root/'src'),str(root/'tests')]))
    child=subprocess.run([sys.executable,'-c',code,str(tmp_path)],env=env,text=True,capture_output=True,timeout=120)
    assert child.returncode==0,child.stdout+child.stderr
    assert json.loads(child.stdout)['regenerations']==2


def semantic_rows(result):
    # Only output locators and run/cell labels differ across fresh directories.
    excluded={'run_id','cell_id','replay_path','runtime_config_path','runtime_result_path'}
    rows=[]
    for original in result['rows']:
        row={k:deepcopy(v) for k,v in original.items() if k not in excluded}
        # The binding hash also covers the output campaign/run/cell labels. Verify its
        # raw payload first, then compare every config value except those
        # labels; do not silently discard a hash that could hide input drift.
        config=json.loads(Path(original['runtime_config_path']).read_text())
        assert canonical_hash(config)==row['binding']['normalized_config_sha256']
        assert config['campaign_id']==config['run_id']==original['run_id']
        config.pop('campaign_id');config.pop('run_id');config.pop('cell_id')
        row['binding']['normalized_config_sha256']=canonical_hash(config)
        rows.append(row)
    return rows


def test_standard_hook_split_and_pending_equal_continuous(tmp_path):
    doc=t.narrowed();doc['search']['max_depth']=3
    full=t.run(tmp_path/'full',doc,maximum=3)
    part=t.run(tmp_path/'part',doc,maximum=1)
    resumed=t.run(tmp_path/'resumed',doc,t.restore(tmp_path/'part/checkpoint.json',tmp_path/'resumed',doc),maximum=3)
    assert t.projection(part)+t.projection(resumed)==t.projection(full)
    assert resumed['selection_log']==full['selection_log']
    assert semantic_rows(part)+semantic_rows(resumed)==semantic_rows(full)
    assert any(row.get('effective_input_sha256') for row in full['rows'])
    class Interrupted(Exception):pass
    def wrap(hook):
        def callback(payload):
            hook(payload)
            if payload['phase']=='planned':raise Interrupted()
        return callback
    with pytest.raises(Interrupted):t.run(tmp_path/'pending',doc,maximum=3,wrapper=wrap)
    state=t.restore(tmp_path/'pending/checkpoint.json',tmp_path/'pending-resumed',doc)
    result=t.run(tmp_path/'pending-resumed',doc,state,maximum=3)
    assert result['selection_log']==full['selection_log']
    assert t.projection(result)==t.projection(full)
    assert semantic_rows(result)==semantic_rows(full)
    assert result['budget']['actual']==result['budget']['attempted']==0


def test_node_expansion_memo_keeps_budget_and_descriptor_in_key():
    doc=t.narrowed();doc['search']['max_depth']=2
    raw=t.fresh(doc)['scheduler'].frontier.to_dict()
    with _frontier_validation_scope():
        a=InputSearchFrontier.from_dict(raw)
        # A changed bound cannot borrow a previously expanded node template.
        limited=deepcopy(raw);limited['settings'].update(max_tasks=1,samples_per_class=2)
        b=InputSearchFrontier.prefix(limited,0)
        assert b.settings['max_tasks']==1
        assert not b.tasks
        assert next(iter(b.nodes.values()))['expansion_status']=='task_budget_blocked'
        assert b.to_dict()==InputSearchFrontier.from_dict(b.to_dict()).to_dict()
        assert a.to_dict()==raw
        # Cache returns are independent even after a consumer mutates tasks.
        c=InputSearchFrontier.prefix(raw,0);c.tasks.clear()
        assert InputSearchFrontier.prefix(raw,0).to_dict()==raw


def test_atomic_json_matches_old_stream_bytes_and_preserves_old_on_failure(tmp_path):
    import io
    from r2f.campaign.checkpoint import _atomic_json
    value={'unicode':'\u4e2d\u6587','float':-0.0,'escape':'\n\\','nested':[True,None,{'z':1,'a':1.25}]}
    expected=io.StringIO();json.dump(value,expected,sort_keys=True,indent=2,allow_nan=False);expected.write('\n')
    path=tmp_path/'atomic.json';_atomic_json(path,value)
    assert path.read_bytes()==expected.getvalue().encode()
    with pytest.raises(ValueError):_atomic_json(path,{'nonfinite':float('nan')})
    assert path.read_bytes()==expected.getvalue().encode()
    assert not list(tmp_path.glob('.checkpoint-*.tmp'))



def test_alias_and_non_json_frontiers_keep_original_reconstruction_path():
    raw=t.fresh(t.narrowed())['scheduler'].frontier.to_dict()
    alias=deepcopy(raw)
    digest=alias['roots'][0]['content_sha256']
    alias['nodes'][digest]['entry']=alias['roots'][0]
    assert _memo_key(alias) is None
    with patch.object(InputSearchFrontier,'prefix',wraps=InputSearchFrontier.prefix) as replay:
        with _frontier_validation_scope():
            assert InputSearchFrontier.from_dict(alias).to_dict()==raw
            assert InputSearchFrontier.from_dict(alias).to_dict()==raw
            assert replay.call_count==2
    not_json=deepcopy(raw);not_json['journal']=tuple(not_json['journal'])
    assert _memo_key(not_json) is None
    with patch.object(InputSearchFrontier,'prefix',wraps=InputSearchFrontier.prefix) as replay:
        with _frontier_validation_scope():
            for _ in range(2):
                with pytest.raises(ValueError,match='frontier state mismatch'):
                    InputSearchFrontier.from_dict(not_json)
            assert replay.call_count==2


def test_model_validated_alias_handoff_rechecks_graph_values_and_detaches():
    from r2f.campaign.search_frontier import _remember_model_validated_frontier
    doc=t.narrowed();doc['search'].update(samples_per_class=2,max_depth=1)
    state=t.fresh(doc);raw=state['scheduler'].frontier.to_dict()
    assert _memo_key(raw) is None  # Normal hook slots share a request dict.
    with patch.object(InputSearchFrontier,'prefix',wraps=InputSearchFrontier.prefix) as replay:
        with _frontier_validation_scope():
            verified=InputSearchFrontier.from_dict(raw)
            verified.validate_model(profile=doc,seeds=state['corpus'],registry=t.registry_for_topology())
            _remember_model_validated_frontier(raw,verified)
            a=InputSearchFrontier.from_dict(raw);assert replay.call_count==1
            a.tasks[0]['status']='corrupt consumer'
            assert InputSearchFrontier.from_dict(raw).to_dict()==verified.to_dict()
            # JSON values are unchanged but alias topology is different: no
            # old handoff may be used, and the full original path must run.
            raw['tasks'][0]['mutation_request']=deepcopy(raw['tasks'][0]['mutation_request'])
            assert InputSearchFrontier.from_dict(raw).to_dict()==verified.to_dict()
            assert replay.call_count==2
            raw['tasks'][0]['mutation_request']['context']['boundary_class']='forged'
            with pytest.raises(ValueError):InputSearchFrontier.from_dict(raw)
            assert replay.call_count==3
    assert _VALIDATION_MEMO.get() is None
