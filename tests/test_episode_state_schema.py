"""Topology episodes use the existing declared topology abstraction, not pair coercion."""
from copy import deepcopy
from dataclasses import replace
import pytest
from r2f.campaign.episode import Episode, EpisodeStore, EpisodeError, run_episode
from r2f.campaign.episode_replay import _subset_episode
from r2f.core.state_signature import STATE_ABSTRACTION_VERSION, state_id
from r2f.core.topology_state import SCHEMA, abstract_topology_state
from test_episode import action, episode, Clock, state as pair_state


def topology_state(generation=1):
    program = {'schema': 'r2f.s1-topology-program/v1', 'endpoints': [
        {'logical_id': 'w0', 'role': 'writer'}, {'logical_id': 'r0', 'role': 'reader'}], 'actions': []}
    facts = {'endpoints': {'w0': {'generation': 1, 'liveness': 'alive'},
                          'r0': {'generation': generation, 'liveness': 'alive'}},
             'pairs': [{'writer': 'w0', 'reader': 'r0', 'compatibility': 'compatible',
                        'observed_match': 'bidirectional', 'delivery_closure': 'closed'}],
             'network_phase': 'normal', 'recovery_cycle_bucket': '0', 'observation_complete': True}
    return abstract_topology_state(program, facts)


def topology_episode():
    before, after = topology_state(), topology_state(2)
    return replace(episode(), initial_state=before, state_schema=SCHEMA,
                   allowed_state_ids=[state_id(before, schema=SCHEMA), state_id(after, schema=SCHEMA)])


def test_topology_state_schema_roundtrip_and_initial_identity():
    ep = topology_episode()
    assert ep.initial_state_id == state_id(ep.initial_state, schema=SCHEMA)
    assert Episode.from_dict(ep.to_dict()).to_dict() == ep.to_dict()
    assert ep.to_dict()['state_schema'] == SCHEMA


def test_pair_default_serialization_and_identity_remain_legacy():
    ep = episode()
    assert ep.state_schema == STATE_ABSTRACTION_VERSION
    assert 'state_schema' not in ep.to_dict()
    assert Episode.from_dict(ep.to_dict()).to_dict() == ep.to_dict()


def test_topology_run_preserves_declared_transition(tmp_path):
    ep = topology_episode()
    out = run_episode(ep, perform=lambda action, state: dict(status='executed', execution_stage='judged',
                      after_state=topology_state(2), oracle_verdict='pass', rule_id='declared-fixture'), clock=Clock())
    assert out['stop_reason'] == 'episode_completed'
    assert out['state_schema'] == SCHEMA
    assert out['receipts'][0]['before_state_id'] == state_id(topology_state(), schema=SCHEMA)
    assert out['receipts'][0]['after_state_id'] == state_id(topology_state(2), schema=SCHEMA)
    store = EpisodeStore(tmp_path)
    store.add(ep, out)
    assert store.load(ep.episode_id)['execution'] == out


def test_cross_schema_observation_is_not_silently_projected():
    ep = topology_episode()
    with pytest.raises(ValueError):
        run_episode(ep, perform=lambda action, state: dict(status='executed', execution_stage='judged',
                    after_state=pair_state(), oracle_verdict='pass', rule_id='fixture'), clock=Clock())


def test_state_outside_declared_topology_model_stops_episode():
    ep = topology_episode()
    out = run_episode(ep, perform=lambda action, state: dict(status='executed', execution_stage='judged',
                      after_state=topology_state(3), oracle_verdict='pass', rule_id='fixture'), clock=Clock())
    assert out['stop_reason'] == 'state_outside_allowed_model'


def test_topology_action_exception_keeps_typed_before_receipt():
    ep = topology_episode()
    def fail(action, state):
        raise RuntimeError('controlled tool failure')
    out = run_episode(ep, perform=fail, clock=Clock())
    assert out['stop_reason'] == 'tool_failure'
    assert out['receipts'][0]['before_state_id'] == ep.initial_state_id
    assert out['receipts'][0]['tool_error'] is True


def test_reduced_episode_preserves_schema():
    ep = topology_episode()
    reduced = _subset_episode(ep, [ep.actions[0].action_id])
    assert reduced.state_schema == SCHEMA
    assert reduced.initial_state_id == ep.initial_state_id


def test_unknown_schema_rejected_without_new_state_machine():
    with pytest.raises(ValueError):
        replace(episode(), state_schema='unknown-schema')


def test_topology_state_requires_explicit_schema_not_auto_detection():
    with pytest.raises(ValueError):
        replace(episode(), initial_state=topology_state())


@pytest.mark.parametrize('replacement', ['missing', STATE_ABSTRACTION_VERSION, 'unknown-schema', None])
def test_execution_schema_bound_at_store_add_load_and_replay(tmp_path, replacement):
    import json
    from r2f.campaign.episode import EpisodeStoreError
    from r2f.campaign.episode_replay import replay_episode, EpisodeReplayError
    from r2f.core.evidence import canonical_hash
    ep = topology_episode()
    perform = lambda action, state: dict(status='executed', execution_stage='judged',
                    after_state=topology_state(2), oracle_verdict='pass', rule_id='fixture')
    out = run_episode(ep, perform=perform, clock=Clock())
    broken = deepcopy(out)
    if replacement == 'missing':
        broken.pop('state_schema')
    else:
        broken['state_schema'] = replacement
    broken['execution_sha256'] = canonical_hash({k:v for k,v in broken.items() if k != 'execution_sha256'})
    store = EpisodeStore(tmp_path)
    with pytest.raises(EpisodeStoreError, match='state schema'):
        store.add(ep, broken)
    store.add(ep, out)
    path = tmp_path / 'episodes' / (ep.episode_id + '.json')
    record = json.loads(path.read_text()); record['execution'] = broken
    record['record_sha256'] = canonical_hash({k:v for k,v in record.items() if k != 'record_sha256'})
    path.write_text(json.dumps(record))
    with pytest.raises(EpisodeStoreError, match='state schema'):
        store.load(ep.episode_id)
    with pytest.raises(EpisodeReplayError, match='state schema'):
        replay_episode(ep, broken, mode='health', perform=perform, clock=Clock(), reduce=False)
