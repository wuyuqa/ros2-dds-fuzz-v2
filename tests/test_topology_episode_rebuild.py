"""Authored offline seams only. These fixtures cannot qualify DDS execution."""
from copy import deepcopy
import hashlib
import json
from pathlib import Path

import pytest

from r2f.campaign.topology_episode import audit_topology_episode
from r2f.core.evidence import canonical_hash
from r2f.campaign.topology_episode_rebuild import checkpoint_request, load_checkpoint
from r2f.modules.s1.fuzz_mutators_v2 import continuous_episode_program
from r2f.runtime.topology_pair import run_topology
from r2f.runtime.topology_worker import OfflineTopologyBackend


@pytest.fixture
def authored_projector(monkeypatch):
    """An explicitly authored COMPLETE fixture, never execution evidence.

    The real offline projector correctly reports unmeasured environment/peer
    observations as unknown. Replace only those observation values for unit
    tests of reconstruction gating. Real raw validation is tested live, not by
    treating these injected states as DDS measurements.
    """
    import r2f.campaign.topology_state_projection as module
    original = module.project_topology_state
    def authored(*args, **kwargs):
        result = original(*args, **kwargs)
        if result['receipt']['status'] == 'invalid':
            return result
        for side in ('state_before', 'state_after'):
            state = result[side]
            state['network_phase'] = 'normal'
            state['recovery_cycle_bucket'] = '0'
            for pair in state['pairs']:
                if pair['compatibility'] == 'unknown': pair['compatibility'] = 'compatible'
                if pair['observed_match'] == 'unknown': pair['observed_match'] = 'bidirectional'
            if all(p['delivery_closure'] in {'closed', 'not_applicable', 'no_obligation'} for p in state['pairs']):
                state['observation_completeness'] = 'complete'
        assert result['receipt']['execution_evidence'] is False
        assert result['receipt']['fixture'] is True
        return result
    monkeypatch.setattr(module, 'project_topology_state', authored)
    monkeypatch.setattr('r2f.campaign.topology_episode.project_topology_state', authored)
    monkeypatch.setattr('r2f.campaign.topology_episode_rebuild.project_topology_state', authored)
    return authored


@pytest.fixture
def origin(tmp_path, authored_projector):
    program = continuous_episode_program()
    config = {'backend': 'offline_fake', 'run_id': 'origin', 'domain_id':191,
        'source_repo':'/tmp/fixture-src','rmw':'rmw_cyclonedds_cpp','topology_program':program}
    root = tmp_path/'origin';root.mkdir()
    (root/'config.json').write_text(json.dumps(config))
    runtime = run_topology(program,config,root)
    assert runtime['status']=='executed',runtime['reason']
    (root/'runtime_result.json').write_text(json.dumps(runtime))
    request = checkpoint_request(root/'config.json',root/'runtime_result.json','checkpoint_1')
    return program,config,runtime,request


def test_rebuild_validates_checkpoint_before_actual_suffix_in_fresh_directory(tmp_path,origin):
    program,config,original,request=origin
    original_bytes=Path(original['raw_path']).read_bytes()
    new_config=dict(config,run_id='reconstruction',domain_id=192,episode_rebuild=request)
    runtime=run_topology(program,new_config,tmp_path/'new')
    assert runtime['status']=='executed',runtime['reason']
    validation=next(e for e in runtime['events'] if e['event']=='topology_episode_rebuild_validated')
    suffix=[e for e in runtime['events'] if e['event']=='topology_action_begin' and e['action_index']>5]
    assert len(suffix)==8
    assert all(e['event_index']>validation['event_index'] for e in suffix)
    assert validation['validation']['state_match'] is True
    assert validation['validation']['fixture'] is True
    assert validation['validation']['DDS_memory_restored'] is False
    audited=audit_topology_episode(program,runtime,new_config)
    assert audited['structural_valid'],audited
    reconstruction=audited['checkpoint_reconstruction']
    assert reconstruction['validated'] and reconstruction['prefix_actions_reexecuted']==6
    assert reconstruction['suffix_actions_executed']==8
    assert reconstruction['execution_evidence'] is False
    assert audited['candidate_eligible'] is False
    assert Path(original['raw_path']).read_bytes()==original_bytes


def test_state_mismatch_preserves_checkpoint_action_and_never_executes_suffix(tmp_path,origin,monkeypatch):
    program,config,original,request=origin
    from r2f.campaign import topology_episode_rebuild as module
    real=module.validate_reconstructed_checkpoint
    def mismatch(loaded,receipt,events):
        bad=deepcopy(loaded);bad['checkpoint_state']['network_phase']='restore_pending'
        return real(bad,receipt,events)
    monkeypatch.setattr(module,'validate_reconstructed_checkpoint',mismatch)
    runtime=run_topology(program,dict(config,episode_rebuild=request),tmp_path/'new')
    assert runtime['status']=='aborted'
    assert runtime['episode']['execution']['stop_reason']=='receipt_observer_tool_failure'
    assert runtime['episode']['execution']['receipts'][-1]['action_id']=='checkpoint_1'
    assert not any(e['event']=='topology_action_begin' and e['action_index']>5 for e in runtime['events'])
    assert runtime['cleanup_receipt']['closed'] is True


def test_invalid_progress_fails_before_backend_prepare_or_action(tmp_path,origin):
    program,config,original,request=origin
    Path(request['progress']['path']).write_text('{}')
    class NoPrepare(OfflineTopologyBackend):
        def prepare(self,*args):
            raise AssertionError('invalid checkpoint must not reach backend prepare')
    runtime=run_topology(program,dict(config,episode_rebuild=request),tmp_path/'new',backend=NoPrepare())
    assert runtime['status']=='blocked'
    assert 'reference_hash_mismatch:progress' in runtime['reason']
    assert runtime['raw_path'] is None


@pytest.mark.parametrize('fault',['source','rmw','program','non_checkpoint','no_suffix','bool_sequence','progress_rehash_state'])
def test_origin_binding_or_malformed_request_is_not_a_checkpoint(origin,fault):
    program,config,runtime,request=origin
    cfg=deepcopy(config);req=deepcopy(request)
    if fault=='source': cfg['source_manifest']={'head':'different'}
    elif fault=='rmw': cfg['rmw']='rmw_fastrtps_cpp'
    elif fault=='program': cfg['topology_program']['episode']['max_seconds']=179
    elif fault=='non_checkpoint': req['checkpoint_action_id']='publish_1'
    elif fault=='no_suffix': req['sequence']=len(program['actions'])-1
    elif fault=='bool_sequence': req['sequence']=True
    else:
        path=Path(req['progress']['path']);progress=json.loads(path.read_text())
        progress['receipts'][-1]['after_state']['network_phase']='restore_pending'
        progress['record_sha256']=canonical_hash(
            {k:v for k,v in progress.items() if k!='record_sha256'})
        path.write_text(json.dumps(progress));req['progress']['sha256']=hashlib.sha256(path.read_bytes()).hexdigest()
    with pytest.raises((ValueError,IndexError)):
        load_checkpoint(req,program,cfg)


def test_incomplete_real_offline_observation_cannot_be_upgraded(tmp_path):
    program=continuous_episode_program();cfg={'backend':'offline_fake','run_id':'incomplete',
        'domain_id':191,'rmw':'rmw_cyclonedds_cpp','topology_program':program}
    (tmp_path/'config.json').write_text(json.dumps(cfg));runtime=run_topology(program,cfg,tmp_path)
    (tmp_path/'runtime_result.json').write_text(json.dumps(runtime))
    request=checkpoint_request(tmp_path/'config.json',tmp_path/'runtime_result.json','checkpoint_1')
    with pytest.raises(ValueError,match='origin_checkpoint_incomplete'):
        load_checkpoint(request,program,cfg)


def test_checkpoint_can_be_loaded_without_final_episode_record(origin):
    program,config,runtime,request=origin
    root=Path(runtime['raw_path']).parent/'episode'
    for path in (root/'episodes').glob('*.json'): path.unlink()
    loaded=load_checkpoint(request,program,config)
    assert loaded['progress']['action_count']==6
    assert loaded['checkpoint_state']['observation_completeness']=='complete'


def test_independent_consumer_refuses_missing_validation_before_suffix(tmp_path,origin):
    from r2f.campaign.topology_episode_rebuild import audit_rebuild
    program,config,original,request=origin
    cfg=dict(config,episode_rebuild=request)
    runtime=run_topology(program,cfg,tmp_path/'new')
    execution=runtime['episode']['execution']
    changed=deepcopy(runtime)
    next(e for e in changed['events'] if e['event']=='topology_episode_rebuild_validated')['event']='authored_removed_gate'
    with pytest.raises(ValueError,match='completed_without_rebuild_validation'):
        audit_rebuild(program,changed,cfg,execution)


def test_independent_consumer_refuses_gate_rebound_to_wrong_commit(tmp_path,origin):
    from r2f.campaign.topology_episode_rebuild import audit_rebuild
    program,config,original,request=origin
    cfg=dict(config,episode_rebuild=request)
    runtime=run_topology(program,cfg,tmp_path/'new')
    changed=deepcopy(runtime)
    next(e for e in changed['events'] if e['event']=='topology_episode_rebuild_validated')['checkpoint_commit_ref']=0
    with pytest.raises(ValueError,match='rebuild_validation_mismatch'):
        audit_rebuild(program,changed,cfg,runtime['episode']['execution'])


@pytest.mark.parametrize('origin_real,new_real',[(False,True),(True,False)])
def test_mixed_execution_provenance_cannot_skip_fresh_identity(origin,origin_real,new_real):
    from r2f.campaign.topology_episode_rebuild import validate_reconstructed_checkpoint
    program,config,runtime,request=origin
    loaded=load_checkpoint(request,program,config)
    receipt=deepcopy(loaded['progress']['receipts'][-1])
    loaded['execution_evidence']=origin_real
    receipt['observation']['execution_evidence']=new_real
    with pytest.raises(ValueError,match='checkpoint_execution_provenance_mismatch'):
        validate_reconstructed_checkpoint(loaded,receipt,runtime['events'])


def test_real_preload_rejects_fixture_origin_before_backend_preparation(tmp_path,origin,monkeypatch):
    """Mark a stub as real solely to prove pre-action rejection, not execution."""
    import r2f.runtime.topology_pair as module
    program,config,runtime,request=origin
    class ForbiddenRealStub(OfflineTopologyBackend):
        real_ros=True
        backend_name='ros2_public_api'
        def prepare(self,*args):
            raise AssertionError('fixture checkpoint must not reach real prepare')
    # This isolated unit seam does not create ROS or claim route admission.
    monkeypatch.setattr('r2f.runtime.topology_route.validate_topology_route',
        lambda cfg: type('BoundPlan',(),{'final_config':{'topology_program':program}})())
    loaded=load_checkpoint(request,program,config)
    monkeypatch.setattr('r2f.campaign.topology_episode_rebuild.load_checkpoint',lambda *a:loaded)
    result=module.run_topology(program,dict(config,backend=None,live_execution_go=True,episode_rebuild=request),
        tmp_path/'refused-real',backend=ForbiddenRealStub())
    assert result['status']=='blocked'
    assert 'real_runtime_requires_real_checkpoint_origin' in result['reason']
    assert result['raw_path'] is None


def test_late_origin_dependency_failure_revokes_all_provisional_qualification(tmp_path,origin):
    program,config,original,request=origin
    cfg=dict(config,episode_rebuild=request)
    runtime=run_topology(program,cfg,tmp_path/'new')
    assert runtime['status']=='executed'
    valid=audit_topology_episode(program,runtime,cfg)
    assert valid['structural_valid'] and valid['completed']
    Path(request['progress']['path']).write_text('{}')
    rejected=audit_topology_episode(program,runtime,cfg)
    assert rejected['status']=='invalid'
    assert rejected['structural_valid'] is False
    assert rejected['completed'] is False
    assert rejected['execution_evidence'] is False
    assert rejected['candidate_eligible'] is False
    assert rejected['checkpoint_observations_complete'] is False
    assert rejected['reset_verified'] is False
    assert rejected['steps']  # diagnostics retained, not evidence erased
    assert 'reference_hash_mismatch:progress' in rejected['reasons'][0]
