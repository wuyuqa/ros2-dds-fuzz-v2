"""Existing unified replay with authored offline checkpoint observations only."""
from copy import deepcopy
import json
from pathlib import Path
from unittest.mock import patch

from r2f.campaign import replay
from test_topology_cycle import SOURCE, run_cycle, offline_runtime, fresh
from test_topology_episode_rebuild import authored_projector


def test_existing_replay_preserves_input_and_records_checkpoint_execution_metadata(tmp_path, authored_projector):
    repo=Path(__file__).resolve().parents[1]
    profile=json.loads((repo/'config/s1_fuzz/profiles/model-bfs-topology-continuous-episode.json').read_text())
    from r2f.campaign.scheduler import ExploratoryScheduler
    from r2f.campaign.search_frontier import search_settings
    from r2f.campaign.topology_search import descriptor
    state=fresh(profile)
    state['scheduler']=ExploratoryScheduler(state['corpus'],state['feedback'],rng_seed=5,warmup=0,policy='bfs')
    state['scheduler'].configure_search(search_settings(profile['search']),
        descriptor(profile['topology_search'],profile['operator_whitelist']))
    original=run_cycle(tmp_path/'original',profile,state=state,runtime=offline_runtime([]))
    row=original['rows'][0]
    path=Path(row['replay_path']);oldbytes=path.read_bytes();captured=[]
    with patch.object(replay,'source_manifest',return_value=deepcopy(SOURCE)):
        report=replay.replay_cell(path,output_dir=tmp_path/'replay',domain_id=81,
            runtime=offline_runtime(captured),episode_checkpoint='checkpoint_1')
        audit=replay.audit_replay(report['report_path'])
    assert audit['valid'],audit
    assert captured and len(captured)==1
    cfg=captured[0]
    assert cfg['effective_input_sha256']==row['effective_input_sha256']
    assert cfg['topology_program']==row['final_input']['topology_program']
    assert cfg['episode_rebuild']['checkpoint_action_id']=='checkpoint_1'
    assert 'episode_rebuild' not in cfg['effective_input']
    runtime=json.loads((tmp_path/'replay'/'runtime_returned.json').read_text())
    assert runtime['status']=='executed',runtime['reason']
    validation=next(e for e in runtime['events'] if e['event']=='topology_episode_rebuild_validated')
    assert validation['validation']['fixture'] is True
    assert report['episode_checkpoint']=='checkpoint_1'
    assert report['real_ros_execution'] is False and report['status']!='pass'
    assert path.read_bytes()==oldbytes
