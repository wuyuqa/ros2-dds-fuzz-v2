"""Canonical scripts/run_s1.py topology BFS, strictly offline; no second wrapper.

In-process CLI tests use an explicit source fixture and runtime tripwires.
A separate retained subprocess check exercises the unmodified source gate.
"""
from copy import deepcopy
import importlib.util
import json
from pathlib import Path
from unittest.mock import Mock

import pytest

from test_model_search_topology import narrowed
from test_model_search_integration import SOURCE
from test_model_search_frontier import profile as f1_profile

ROOT = Path(__file__).resolve().parents[1]
CYCLE = 'topology-authored'


@pytest.fixture
def cli(monkeypatch):
    spec = importlib.util.spec_from_file_location('topology_canonical_cli', ROOT/'scripts/run_s1.py')
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    monkeypatch.setattr(module,'source_manifest',lambda _repo:deepcopy(SOURCE))
    monkeypatch.setattr('r2f.campaign.runner.source_manifest',lambda _repo:deepcopy(SOURCE))
    runtime = Mock(side_effect=AssertionError('canonical topology test must never construct a runtime'))
    campaign = Mock(side_effect=AssertionError('canonical topology test must never dispatch required campaign'))
    monkeypatch.setattr(module,'runtime_for_boundary',runtime)
    monkeypatch.setattr(module,'run_campaign',campaign)
    yield module
    runtime.assert_not_called()
    campaign.assert_not_called()


def document():
    doc=narrowed()
    doc['search'].update(max_depth=3,max_nodes=8,max_tasks=16)
    doc['budget']['max_inputs']=3
    return doc


def arguments(tmp_path,name,doc,maximum):
    path=tmp_path/'profile.json'
    path.write_text(json.dumps(doc))
    return ['--output-dir',str(tmp_path/name),'--exploratory-profile',str(path),
            '--cycle-id',CYCLE,'--max-inputs',str(maximum),'--seed','41']


def read(out):
    return json.loads((out/'cycle.json').read_text())


def hashes(result):
    return [(row['content_sha256'],row['effective_input_sha256'],row['mutation_plan'],row['input_search'])
            for row in result['rows']]


@pytest.mark.parametrize('explicit_policy',[False,True])
def test_standard_cli_plans_topology_exact_task_and_capsule(cli,tmp_path,capsys,explicit_policy):
    doc=document()
    argv=arguments(tmp_path,'planned',doc,1)
    if explicit_policy:
        argv+=['--scheduler-policy','bfs']
    assert cli.main(argv)==0
    result=read(tmp_path/'planned')
    summary=json.loads(capsys.readouterr().out)
    assert summary['search_summary']==result['search_summary']
    assert result['abort'] is None and result['execute'] is False
    assert result['budget']['actual']==result['budget']['attempted']==result['budget']['executed']==0
    assert result['feedback']['events']==[]
    frontier=result['selection_log']['frontier']
    assert frontier['schema']=='r2f.input-search-frontier/topology-v1'
    assert frontier['topology_descriptor']['schema']=='r2f.topology-search/v1'
    selection=result['selection_log']['selections'][0]
    assert selection['mutation_request']=={key:doc['topology_search']['samples'][0][key]
                                           for key in ('operator_id','slot_id','context')}
    assert selection['task_kind']=='topology_request'
    assert 'mutation_route' not in selection and 'sampling_kind' not in selection
    row=result['rows'][0]
    assert row['execution_stage']=='planned' and row['actual_run'] is False
    capsule=json.loads((tmp_path/'planned/cases'/row['cell_id']/'capsule.json').read_text())
    assert capsule['execution'] is None
    assert row['input_search']['execution'] is None
    operation=row['mutation_plan']['operations'][0]
    assert operation['parameters']['request']==selection['mutation_request']
    assert not any(event['action']=='submit' for event in frontier['journal'])


def test_standard_cli_split_resume_preserves_task_hash_and_full_replay(cli,tmp_path):
    doc=document()
    assert cli.main(arguments(tmp_path,'full',doc,3))==0
    assert cli.main(arguments(tmp_path,'part',doc,1))==0
    assert cli.main(arguments(tmp_path,'resumed',doc,3)+
                    ['--resume-from',str(tmp_path/'part/checkpoint.json')])==0
    full,part,resumed=(read(tmp_path/name) for name in ('full','part','resumed'))
    assert hashes(part)+hashes(resumed)==hashes(full)
    assert resumed['selection_log']==full['selection_log']
    assert [event['derivation_depth'] for event in resumed['selection_log']['selections']]==[1,2,3]
    assert resumed['budget']['actual']==resumed['budget']['attempted']==0
    assert not any(event['action']=='submit' for event in resumed['selection_log']['frontier']['journal'])
    for result,name in ((part,'part'),(resumed,'resumed')):
        for row in result['rows']:
            capsule=json.loads((tmp_path/name/'cases'/row['cell_id']/'capsule.json').read_text())
            assert capsule['execution'] is None


def test_standard_cli_rejection_keeps_requested_pair_and_raw_model_reason(cli,tmp_path):
    doc=narrowed('relation.pair','S1.topology.qos.reliability',{'pair':'BB'})
    doc['search']['max_depth']=1
    assert cli.main(arguments(tmp_path,'rejected',doc,1))==0
    result=read(tmp_path/'rejected')
    assert result['abort'] is None and len(result['rejected'])==1
    row=result['rows'][0]
    assert row['generation_status']=='rejected' and row['actual_run'] is False
    assert row['mutation_plan'] is None
    assert row['input_search']['mutation_request']['context']=={'pair':'BB'}
    task=result['selection_log']['frontier']['tasks'][0]
    assert task['generation_outcome']=='generation_rejected'
    assert task['task_applicability']['status']=='not_applicable'
    assert task['task_applicability']['reason']=='generation_no_change'
    assert 'equals the parent' in task['rejection']['reason']
    journal=result['selection_log']['frontier']['journal']
    assert [e['action'] for e in journal]==['select','generation_rejected','finish']
    assert result['budget']['actual']==result['budget']['generated']==0
    # Restore the terminal rejection via the ordinary loader/CLI, which must
    # regenerate the rejection before trusting the retained model state.
    assert cli.main(arguments(tmp_path,'rejection-resume',doc,2)+
                    ['--resume-from',str(tmp_path/'rejected/checkpoint.json')])==0
    restored=read(tmp_path/'rejection-resume')
    assert restored['selection_log']==result['selection_log']
    assert restored['budget']['selected']==1


def test_standard_cli_guard_negative_is_terminal_without_capsule_or_submit(cli,tmp_path):
    doc=narrowed('filter.expr','S1.topology.filter',{'filter_variant':'invalid'})
    assert cli.main(arguments(tmp_path,'guard',doc,1))==0
    result=read(tmp_path/'guard')
    row=result['rows'][0]
    task=result['selection_log']['frontier']['tasks'][0]
    assert row['reason_code']=='model_guard_terminal'
    assert task['generation_outcome']=='guard_terminal' and task['status']=='rejected'
    assert task['generated_record']['executable'] is False
    assert len(result['selection_log']['frontier']['nodes'])==1
    assert result['budget']['actual']==result['budget']['generated']==0
    assert not list((tmp_path/'guard/cases').glob('*/capsule.json'))
    assert not any(e['action']=='submit' for e in result['selection_log']['frontier']['journal'])


@pytest.mark.parametrize('damage',['chain','unconsumed','delivery','f1_sampling','schema','policy'])
def test_standard_cli_strict_profile_gates_fail_before_runner(cli,tmp_path,monkeypatch,damage):
    doc=document()
    if damage=='chain':
        doc['chain_length']=2
    elif damage=='unconsumed':
        doc['topology_search']['samples'][0]['context']['delay_sec']=0.1
    elif damage=='delivery':
        doc['topology_search']['samples'][0]['operator_id']='env.drop'
    elif damage=='f1_sampling':
        doc['sampling_kinds']=['representative']
    elif damage=='schema':
        doc['topology_search']['schema']='future'
    argv=arguments(tmp_path,'refused',doc,1)
    if damage=='policy':
        argv+=['--scheduler-policy','fair']
    cycle=Mock(side_effect=AssertionError('invalid profile reached runner'))
    monkeypatch.setattr(cli,'run_exploratory_cycle',cycle)
    with pytest.raises(SystemExit) as error:
        cli.main(argv)
    assert error.value.code==2
    cycle.assert_not_called()
    assert not (tmp_path/'refused').exists()


@pytest.mark.parametrize('boundary',['host','isolated_netns'])
def test_standard_cli_topology_execute_dispatch_preserves_bfs_config_without_runtime_call(cli,tmp_path,monkeypatch,boundary):
    # The former blanket plan-only rejection is intentionally superseded.
    # Resolve a labelled token, intercept dispatch, and never call a runtime.
    token=Mock(side_effect=AssertionError('canonical runtime token must never be called'))
    resolver=Mock(return_value=token)
    monkeypatch.setattr(cli,'runtime_for_boundary',resolver)
    class DispatchIntercepted(Exception):
        pass
    cycle=Mock(side_effect=DispatchIntercepted())
    monkeypatch.setattr(cli,'run_exploratory_cycle',cycle)
    doc=document()
    argv=arguments(tmp_path,'intercepted-execute',doc,1)+[
        '--execute','--runtime-boundary',boundary]
    with pytest.raises(DispatchIntercepted):
        cli.main(argv)
    resolver.assert_called_once_with(boundary)
    token.assert_not_called()
    cycle.assert_called_once()
    submitted=cycle.call_args.kwargs
    assert submitted['execute'] is True and submitted['runtime'] is token
    assert submitted['runtime_boundary']==boundary
    assert submitted['profile']==doc and submitted['max_inputs']==1
    assert cycle.call_args.args==(tmp_path/'intercepted-execute',)
    scheduler=submitted['scheduler']
    assert scheduler.policy=='bfs' and scheduler.rng_seed==41
    assert scheduler.frontier.topology_descriptor['samples']==doc['topology_search']['samples']
    assert scheduler.frontier.settings==doc['search']
    assert not scheduler.selection_log
    assert not (tmp_path/'intercepted-execute').exists()


def test_standard_cli_resume_rejects_changed_explicit_request(cli,tmp_path):
    doc=document()
    assert cli.main(arguments(tmp_path,'part',doc,1))==0
    changed=deepcopy(doc)
    changed['topology_search']['samples'][0]['context']['boundary_class']='one'
    with pytest.raises(SystemExit) as error:
        cli.main(arguments(tmp_path,'bad-resume',changed,2)+
                 ['--resume-from',str(tmp_path/'part/checkpoint.json')])
    assert error.value.code==2
    assert not (tmp_path/'bad-resume').exists()


def test_standard_cli_f1_frontier_v1_shape_stays_unchanged(cli,tmp_path):
    doc=f1_profile()
    argv=arguments(tmp_path,'f1',doc,1)
    argv[argv.index('--cycle-id')+1]='common-baseline'
    assert cli.main(argv)==0
    result=read(tmp_path/'f1')
    frontier=result['selection_log']['frontier']
    assert frontier['schema']=='r2f.input-search-frontier/v1'
    assert 'topology_descriptor' not in frontier
    assert 'mutation_route' in result['selection_log']['selections'][0]
    assert 'task_kind' not in result['selection_log']['selections'][0]
    assert result['budget']['actual']==0
