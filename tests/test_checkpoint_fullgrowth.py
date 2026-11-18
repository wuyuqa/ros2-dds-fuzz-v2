"""Exact-scope optimization contracts. Offline only, no runtime execution."""
from copy import deepcopy
import json
import os
from pathlib import Path
import subprocess
import sys
from unittest.mock import patch

import pytest
import test_model_search_topology as fixture
from r2f.campaign.corpus import Corpus
from r2f.campaign.scheduler import _restore_checkpoint_corpus
from r2f.campaign.search_frontier import _frontier_validation_scope, _VALIDATION_MEMO
from r2f.core.evidence import canonical_hash


def test_restore_exact_value_reuses_but_returns_detached_copy():
    data = fixture.fresh(fixture.narrowed())['corpus'].to_dict()
    original = Corpus.from_dict
    with patch.object(Corpus, 'from_dict', wraps=original) as calls:
        with _frontier_validation_scope():
            one = _restore_checkpoint_corpus(data)
            one.history[0]['reason'] = 'caller mutation'
            two = _restore_checkpoint_corpus(deepcopy(data))
            assert two.to_dict() == data
            assert calls.call_count == 1
        _restore_checkpoint_corpus(data)
        assert calls.call_count == 2
    assert _VALIDATION_MEMO.get() is None


def test_changed_value_with_same_supplied_hash_does_not_hit():
    data = fixture.fresh(fixture.narrowed())['corpus'].to_dict()
    with _frontier_validation_scope():
        _restore_checkpoint_corpus(data)
        # Deliberately leave the supplied content identity unchanged.
        data['entries'][0]['final_config']['unexpected'] = True
        with pytest.raises(ValueError):
            _restore_checkpoint_corpus(data)


def test_nested_scope_and_exception_cannot_reuse_outer_cache():
    data = fixture.fresh(fixture.narrowed())['corpus'].to_dict()
    original = Corpus.from_dict
    with patch.object(Corpus, 'from_dict', wraps=original) as calls:
        with _frontier_validation_scope():
            _restore_checkpoint_corpus(data)
            with pytest.raises(RuntimeError):
                with _frontier_validation_scope():
                    _restore_checkpoint_corpus(data)
                    raise RuntimeError('scope exit')
            _restore_checkpoint_corpus(data)
            assert calls.call_count == 2
    assert _VALIDATION_MEMO.get() is None


def test_changed_model_parents_do_not_hit():
    data = fixture.fresh(fixture.narrowed())['corpus'].to_dict()
    with _frontier_validation_scope():
        _restore_checkpoint_corpus(data, model_parents={})
        with pytest.raises(ValueError):
            _restore_checkpoint_corpus(data, model_parents={'f'*64: deepcopy(data['entries'][0])})


@pytest.fixture(scope='module')
def checkpoint_case(tmp_path_factory):
    output = tmp_path_factory.mktemp('fullgrowth')
    doc = fixture.narrowed(op='timing.shift', slot='S1.topology.qos.deadline',
                          context={'offset_sec':1.0}, seed='topology.writer_only')
    doc['search'].update(max_depth=1, samples_per_class=1)
    doc['topology_search']['samples'] = [
        dict(operator_id='timing.shift', slot_id='S1.topology.qos.deadline',
             class_id=f'offset_{i}', context={'offset_sec':float(i)}) for i in (1, 2)]
    result = fixture.run(output/'run', doc, maximum=2)
    assert result['budget']['generated'] == result['budget']['unique_inputs'] == 2
    assert len(result['selection_log']['frontier']['nodes']) == 3
    return output, doc, result


def test_standard_hook_load_roundtrip(checkpoint_case):
    output, doc, result = checkpoint_case
    restored = fixture.restore(output/'run/checkpoint.json', output/'restore', doc)
    assert restored['scheduler'].to_dict() == result['selection_log']


def _cold_load(path, output, doc, destination):
    profile = output/(destination + '-profile.json')
    profile.write_text(json.dumps(doc))
    code = '''import sys,json
from pathlib import Path
import test_model_search_topology as f
state=f.restore(Path(sys.argv[1]),Path(sys.argv[2]),json.loads(Path(sys.argv[3]).read_text()))
print(json.dumps(state['scheduler'].to_dict(),sort_keys=True))
'''
    env = dict(os.environ, PYTHONPATH=os.pathsep.join((str(fixture.ROOT/'src'), str(fixture.ROOT/'tests'))), PYTHONDONTWRITEBYTECODE='1')
    return subprocess.run([sys.executable,'-c',code,str(path),str(output/destination),str(profile)],env=env,text=True,capture_output=True,timeout=45)


def test_cold_process_rebuild(checkpoint_case):
    output, doc, result = checkpoint_case
    process = _cold_load(output/'run/checkpoint.json',output,doc,'cold-valid')
    assert process.returncode == 0, process.stderr
    assert json.loads(process.stdout) == result['selection_log']


@pytest.mark.parametrize('kind', ['nested_plan','context_count','budget_count','source'])
def test_resigned_nested_tamper_refused_in_cold_process(checkpoint_case, kind):
    output, doc, _ = checkpoint_case
    data = json.loads((output/'run/checkpoint.json').read_text())
    state=data['state']; scheduler=state['scheduler']
    if kind == 'nested_plan':
        event=next(e for e in scheduler['frontier']['journal'] if e['action']=='plan')
        event['record']['mutation_plan']['rng_seed'] += 1
        # Resign the innermost known identity, not just the outer envelope.
        plan=event['record']['mutation_plan']
        if 'plan_sha256' in plan:
            plan['plan_sha256']=canonical_hash({k:v for k,v in plan.items() if k!='plan_sha256'})
    elif kind == 'context_count':
        context=scheduler['selection_contexts'][0]
        context['frontier_journal_count'] += 1
        context['context_sha256']=canonical_hash({k:v for k,v in context.items() if k!='context_sha256'})
    elif kind == 'budget_count':
        state['continuation']['budget']['generated'] += 1
    else:
        data['identity']['source_manifest']['head']='different-source'
    scheduler['selection_identity']=canonical_hash({k:v for k,v in scheduler.items() if k!='selection_identity'})
    data['checkpoint_sha256']=canonical_hash({k:v for k,v in data.items() if k!='checkpoint_sha256'})
    tamper_dir=output/('tampered-'+kind);tamper_dir.mkdir()
    path=tamper_dir/'checkpoint.json';path.write_text(json.dumps(data))
    process=_cold_load(path,output,doc,'cold-'+kind)
    assert process.returncode != 0
    assert 'ValueError' in process.stderr, process.stderr
    expected={'nested_plan':'action_program receipt seed mismatch',
              'context_count':'historical frontier prefix mismatch',
              'budget_count':'planned/generated/attempted/executed budget mismatch',
              'source':'source/target/profile/catalog identity mismatch'}
    assert expected[kind] in process.stderr, process.stderr


def test_hot_writer_reuses_only_pure_generation_cold_loader_regenerates(tmp_path):
    from r2f.campaign import topology_search
    doc=fixture.narrowed(op='timing.shift',slot='S1.topology.qos.deadline',
                         context={'offset_sec':1.0},seed='topology.writer_only')
    doc['search'].update(max_depth=1,samples_per_class=1)
    doc['topology_search']['samples']=[dict(operator_id='timing.shift',slot_id='S1.topology.qos.deadline',
        class_id=f'offset_{i}',context={'offset_sec':float(i)}) for i in (1,2)]
    with patch.object(topology_search,'generate',wraps=topology_search.generate) as generation:
        result=fixture.run(tmp_path/'hot',doc,maximum=2)
        # Two runner generations + two independent model validations. No
        # journal snapshot is skipped, only equal pure re-computations.
        assert generation.call_count == 4
        restored=fixture.restore(tmp_path/'hot/checkpoint.json',tmp_path/'cold',doc)
        assert generation.call_count == 6
    assert restored['scheduler'].to_dict()==result['selection_log']


def test_cold_loader_explicitly_shadows_outer_writer_scope(checkpoint_case):
    from r2f.campaign.search_frontier import _hook_model_validation_scope, _HOOK_MODEL_MEMO
    output,doc,result=checkpoint_case
    poisoned={'not_a_valid_hot_cache':True}
    with _hook_model_validation_scope(poisoned):
        restored=fixture.restore(output/'run/checkpoint.json',output/'nested-cold',doc)
        assert _HOOK_MODEL_MEMO.get() is poisoned
    assert _HOOK_MODEL_MEMO.get() is None
    assert poisoned=={'not_a_valid_hot_cache':True}
    assert restored['scheduler'].to_dict()==result['selection_log']


@pytest.mark.parametrize('change',['record','request'])
def test_hot_model_cache_checks_full_parent_task_and_expected_record(change):
    from r2f.campaign.search_frontier import _hook_model_validation_scope
    doc=fixture.narrowed(op='timing.shift',slot='S1.topology.qos.deadline',
                         context={'offset_sec':1.0},seed='topology.writer_only')
    doc['search'].update(max_depth=1,samples_per_class=1)
    state=fixture.fresh(doc);fixture.generate_one(state,doc)
    frontier=state['scheduler'].frontier
    cache={'identity_key':'test-source-profile','plans':{},'node':{},'noop':{}}
    with _hook_model_validation_scope(cache):
        frontier.validate_model(profile=doc,seeds=state['corpus'],registry=fixture.registry_for_topology())
        assert len(cache['plans'])==1
        if change=='record':
            event=next(e for e in frontier.journal if e['action']=='plan')
            event['record']['mutation_plan']['rng_seed']+=1
        else:
            task=next(t for t in frontier.tasks if t.get('generated_record'))
            task['mutation_request']['context']['offset_sec']=9.0
        with pytest.raises(ValueError):
            frontier.validate_model(profile=doc,seeds=state['corpus'],registry=fixture.registry_for_topology())
