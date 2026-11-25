"""Identity input is sealed by the existing registry/capsule; fixtures stay offline."""
from copy import deepcopy
import json
import time

import pytest

from r2f.core import ReplayCapsule, Scenario
from r2f.modules.s1 import topology_model as model
from r2f.modules.s1.topology_input import make_topology_plan, topology_input_layer_receipts
from r2f.runtime.ros2_pair import run_pair
from r2f.runtime.topology_worker import OfflineTopologyBackend
from r2f.campaign.topology_adapter import TopologyFactProducer
from r2f.modules.s1.topology_oracles import TopologyOracle
from r2f.campaign.replay import _replay_config
from test_topology_cycle import SOURCE, capsule_from_registry


def identity_scenario(mode='fresh_process', readers=1):
    registry, original, _, _ = capsule_from_registry(1)
    program = model.generate_program(7, writers=1, readers=readers)
    program['identity_checks'] = {'initialization': mode}
    plan = make_topology_plan(program, [{'operator_id': 'action.insert'}], rng_seed=7)
    scenario = Scenario(original.target, 'rmw_cyclonedds_cpp', 'mutated', 0, seed=7, mutation_plan=plan)
    prepared = registry.get_services(scenario.target_id).mutation_provider.prepare_plan(scenario, plan)
    capsule = ReplayCapsule.from_scenario(scenario, source_manifest=SOURCE,
        final_input=plan.final_config, input_layer_receipts=prepared['input_layer_receipts'], origin_domain=80)
    return scenario, capsule


@pytest.mark.parametrize('mode', ['fresh_process', 'fork_before_participant'])
def test_capsule_and_public_route_preserve_identity_contract_without_inventing_execution(tmp_path, mode):
    scenario, capsule = identity_scenario(mode)
    restored = ReplayCapsule.from_dict(json.loads(json.dumps(capsule.to_dict())))
    config = _replay_config(restored, 'offline-identity', 80, SOURCE)
    assert config['topology_program']['identity_checks'] == {'initialization': mode}
    runtime = run_pair(config, tmp_path / mode, topology_backend=OfflineTopologyBackend())
    facts = TopologyFactProducer().produce({'events': runtime['events'], 'runtime': runtime, 'config': config})
    result = TopologyOracle().evaluate(scenario, facts)
    (tmp_path / 'derived-report.json').write_text(json.dumps({'facts': facts, 'oracle': result}))
    persisted = json.loads((tmp_path / 'derived-report.json').read_text())
    assert persisted['facts']['participant_identity']['status'] == 'unknown'
    assert 'initialization_observation_missing' in persisted['facts']['participant_identity']['unknown_reasons']
    assert result['verdict'] == 'unknown'
    assert not result['candidate_eligible']
    assert runtime['real_execution'] is False


def test_initialization_order_changes_input_identity_and_never_relabels_after_init_fork_supported():
    _, a = identity_scenario('fresh_process')
    _, b = identity_scenario('fork_before_participant')
    assert a.input != b.input
    program = model.generate_program(7, writers=1, readers=1)
    program['identity_checks'] = {'initialization': 'fork_after_participant'}
    result = model.validate_program(program)
    assert result['status'] == 'invalid'
    assert any('unsupported_initialization_contract' in e['message'] for e in result['errors'])


def ownership(endpoint, participant, iid, domain=80, handles=(10, 30), at=None):
    """Authored public-API values, explicitly NOT DDS observations."""
    at = time.monotonic_ns() - 500 if at is None else at
    root_handle, endpoint_handle = handles
    rows = [{'entity_handle': h, 'guid': guid, 'instance_id': value,
             'return_code': 0, 'guid_return_code': 0}
            for h, guid, value in [(root_handle, participant, iid), (endpoint_handle, endpoint, iid + 100000)]]
    return {'schema': 'r2f.cyclone-participant-ownership/v2', 'status': 'observed',
        'complete': True, 'stable_sample_count': 2, 'domain_id': domain,
        'local_gid': endpoint, 'participant_gid': participant,
        'local_handle': endpoint_handle, 'participant_handle': root_handle, 'parent_chain': [endpoint_handle, root_handle],
        'started_monotonic_ns': at, 'ended_monotonic_ns': at + 20,
        'entity_inventory': [[root_handle, [endpoint_handle]], [endpoint_handle, []]], 'provenance': {'fixture': True},
        'entity_identities': [{'handle': h, 'gid': guid, 'domain_id': domain, 'guid_return_code': 0}
            for h, guid in [(root_handle, participant), (endpoint_handle, endpoint)]],
        'instance_identity': {'schema': 'r2f.cyclone-entity-instance-identity/v1',
            'status': 'observed', 'api': 'dds_get_instance_handle+dds_get_guid',
            'scope': 'cyclonedds_entity_instance_id', 'started_monotonic_ns': at + 5,
            'ended_monotonic_ns': at + 10, 'samples': [deepcopy(rows), deepcopy(rows)]}}


class IdentityFixture(OfflineTopologyBackend):
    """A carrier double; the production adapter still reads serialized raw."""
    def __init__(self, damage=None):
        super().__init__()
        self.damage = damage

    def prepare(self, program, config):
        self.config = config
        return super().prepare(program, config)

    def _emit(self, event, **fields):
        if event == 'endpoint_created':
            identity = {k: fields[k] for k in ('pid', 'pgid', 'process_start_ticks')}
            identity.update(ppid=10, pid_namespace='pid:[123]')
            serial = fields['process_start_ticks']
            phases = []
            base = time.monotonic_ns() - 10000
            for gen in (1, 2):
                participants = []
                started = base + gen * 1000
                for slot in (0, 1):
                    number = serial * 1000 + gen * 10 + slot
                    root, reader = f'{number:032x}', f'{number + 100000:032x}'
                    observed = ownership(reader, root, number, handles=(10 + slot, 30 + slot), at=started + 300)
                    participants.append({'slot': slot, 'ownership': observed,
                        'participant_created': {'guid': root, 'domain_id': 80, 'participant_handle': 10 + slot,
                            'guid_return_code': 0, 'real_execution': True, 'receipt_kind': 'native_participant_created',
                            'created_monotonic_ns': started + 50 + slot * 100, 'monotonic_ns': started + 60 + slot * 100},
                        'reader_created': {'reader_handle': 30 + slot, 'real_execution': True,
                            'receipt_kind': 'native_builtin_reader_created', 'monotonic_ns': started + 80 + slot * 100}})
                phases.append({'generation': gen, 'started_monotonic_ns': started,
                    'observed_monotonic_ns': started + 400, 'closed_monotonic_ns': started + 600,
                    'participants': participants, 'cleanup_complete': True,
                    'cleanup': [{'receipt_kind': 'native_participant_closed', 'cleanup_complete': True,
                        'failures': [], 'monotonic_ns': started + 500,
                        'deleted': [{'kind': 'builtin_reader', 'handle': 30 + slot},
                                    {'kind': 'participant', 'handle': 10 + slot}]} for slot in (1, 0)]})
            probe = {'schema': 'r2f.participant-identity-initialization/v1',
                'contract': {'initialization': 'fresh_process'}, 'status': 'observed',
                'cleanup_complete': True, 'started_monotonic_ns': base - 1000, 'ended_monotonic_ns': base + 9000,
                'before_initialization': {'process': identity,
                    'local_participant_count': 0, 'thread_count': 1, 'library_preloaded': True, 'monotonic_ns': base},
                'processes': [{'identity': identity, 'phases': phases, 'cleanup_complete': True}]}
            if self.damage == 'cleanup_contradiction':
                phases[0]['cleanup'][0].update(failures=[{'return_code': -1}], cleanup_complete=False)
            if self.damage == 'reversed_generations':
                phases[1]['started_monotonic_ns'] = base - 999
            if self.damage == 'missing_generation':
                probe['processes'][0]['phases'].pop()
            if self.config['topology_program']['identity_checks']['initialization'] == 'fork_before_participant':
                fork_probe(probe, self.damage)
            super()._emit('participant_identity_initialization', **identity,
                observation=probe, run_id=self.config['run_id'], domain_id=80)
        if event == 'match_snapshot':
            identity = {k: fields[k] for k in ('logical_id', 'generation', 'pid', 'process_start_ticks', 'endpoint_generation')}
            identity['pgid'] = fields['pid']
            serial = fields['process_start_ticks']
            root = f'{900000 + (0 if self.damage == "collision" else serial):032x}'
            value = ownership(fields['endpoint_gid'], root, 800000 + serial)
            if self.damage == 'zero_iid':
                for sample in value['instance_identity']['samples']:
                    sample[0]['instance_id'] = 0
            if self.damage == 'wrong_endpoint':
                value['local_gid'] = 'f' * 32
            super()._emit('participant_ownership_snapshot', **identity,
                endpoint_gid=fields['endpoint_gid'], command_id=fields['command_id'],
                participant_ownership_observation=value)
        return super()._emit(event, **fields)


@pytest.mark.parametrize('damage,status', [(None, 'consistent'), ('collision', 'conflict'),
    ('zero_iid', 'unknown'), ('wrong_endpoint', 'unknown'), ('missing_generation', 'unknown'),
    ('cleanup_contradiction', 'unknown'), ('reversed_generations', 'unknown')])
def test_identity_observations_are_rebuilt_from_raw_and_fixtures_never_become_candidates(tmp_path, damage, status):
    scenario, capsule = identity_scenario()
    config = _replay_config(capsule, 'offline-identity', 80, SOURCE)
    runtime = run_pair(config, tmp_path, topology_backend=IdentityFixture(damage))
    facts = TopologyFactProducer().produce({'events': runtime['events'], 'runtime': runtime, 'config': config})
    assert facts['diagnostic_valid'], facts.get('evidence_error')
    observed = facts['participant_identity']
    assert observed['status'] == status, observed
    result = TopologyOracle().evaluate(scenario, facts)
    assert not result['candidate_eligible']
    if damage == 'collision':
        assert result['verdict'] == 'fail'
        assert result['forbidden_residuals'] == ['topology.identity_mismatch']
    elif status == 'unknown':
        assert result['verdict'] == 'unknown'
        assert observed['unknown_reasons']
    else:
        assert len(observed['initializations']) == 2
        assert {r['logical_id'] for r in observed['observations']} == {'w0', 'r0'}
    for observation in observed['observations']:
        assert observation['raw_ref']['sha256'] == runtime['raw_sha256']


def test_common_candidate_identity_reopens_raw_and_rejects_a_rehashed_summary_claim(tmp_path):
    from r2f.core.candidate import topology_observation_identity, _validate_observation_identity
    scenario, capsule = identity_scenario()
    config = _replay_config(capsule, 'offline-identity', 80, SOURCE)
    runtime = run_pair(config, tmp_path, topology_backend=IdentityFixture('collision'))
    facts = TopologyFactProducer().produce({'events': runtime['events'], 'runtime': runtime, 'config': config})
    result = TopologyOracle().evaluate(scenario, facts)
    row = {'target_id': scenario.target_id, 'run_id': config['run_id'], 'raw_path': runtime['raw_path'],
           'raw_sha256': runtime['raw_sha256'], 'final_input': capsule.input['effective_input']['final_input'],
           'typed_facts': facts, 'oracle_verdict': result['verdict'], 'oracle': result}
    row['endpoint_identity'] = topology_observation_identity(row)
    _validate_observation_identity(row)
    assert row['endpoint_identity']['participant_identity']['status'] == 'conflict'
    facts['participant_identity']['conflicts'][0]['value'] = 'f' * 32
    row['endpoint_identity'] = topology_observation_identity(row)
    with pytest.raises(ValueError, match='raw recomputation'):
        _validate_observation_identity(row)


def test_identity_residual_uses_existing_provider_assessment_without_unconditional_denial(tmp_path):
    scenario, capsule = identity_scenario()
    config = _replay_config(capsule, 'offline-identity', 80, SOURCE)
    runtime = run_pair(config, tmp_path, topology_backend=IdentityFixture('collision'))
    facts = TopologyFactProducer().produce({'events': runtime['events'], 'runtime': runtime, 'config': config})
    # Explicit schema model ONLY. No canonical/formal qualifier is run or granted.
    modeled = deepcopy(facts)
    modeled.update(fixture=False, source_provenance_qualified=True)
    result = TopologyOracle().evaluate(scenario, modeled)
    checks = result['candidate_assessment']['checks']
    assert checks['typed_residuals'] and checks['not_diagnostic']
    assert not checks['typed_execution_health']
    assert not result['candidate_eligible']


def fork_probe(probe, damage=None):
    """Synthetic concurrent native receipts, never live initialization evidence."""
    parent = probe['processes'][0]
    child = deepcopy(parent)
    child['identity'].update(pid=parent['identity']['pid'] + 100,
        ppid=parent['identity']['pid'], process_start_ticks=parent['identity']['process_start_ticks'] + 100)
    def shift(value, delta):
        if isinstance(value, dict):
            for key, item in value.items():
                if key.endswith('_ns') and type(item) is int:
                    value[key] += delta
                else:
                    shift(item, delta)
        elif isinstance(value, list):
            for item in value:
                shift(item, delta)
    shift(child['phases'], 4000 if damage == 'sequential_child' else 20)
    for phase in child['phases']:
        for row in phase['participants']:
            old = row['ownership']
            root, endpoint = f"{int(old['participant_gid'], 16) + 10000000:032x}", f"{int(old['local_gid'], 16) + 10000000:032x}"
            row['ownership'] = ownership(endpoint, root,
                old['instance_identity']['samples'][0][0]['instance_id'] + 10000000,
                handles=(old['participant_handle'], old['local_handle']), at=old['started_monotonic_ns'])
            row['participant_created']['guid'] = root
    if damage == 'invalid_fork_collision':
        for pphase, cphase in zip(parent['phases'], child['phases']):
            cphase['participants'][0] = deepcopy(pphase['participants'][0])
    probe.update(contract={'initialization': 'fork_before_participant'},
        child_kernel_identity=deepcopy(child['identity']), child_reaped=True, child_returncode=0,
        processes=[parent, child], rendezvous=[])
    for p, c in zip(parent['phases'], child['phases']):
        snapshot = deepcopy(c)
        snapshot.pop('closed_monotonic_ns')
        snapshot.update(cleanup=[], cleanup_complete=False)
        probe['rendezvous'].append(dict(generation=p['generation'], child_phase=snapshot,
            child_identity=deepcopy(child['identity']),
            parent_observed_monotonic_ns=p['observed_monotonic_ns'],
            received_monotonic_ns=max(p['observed_monotonic_ns'], c['observed_monotonic_ns']) + 20,
            release_monotonic_ns=max(p['observed_monotonic_ns'], c['observed_monotonic_ns']) + 30))
    if damage == 'no_rendezvous':
        probe.pop('rendezvous')
    elif damage in ('stale_snapshot', 'invalid_fork_collision'):
        probe['rendezvous'][0]['child_phase']['participants'][0]['reader_created']['reader_handle'] += 1
    elif damage == 'wrong_child':
        probe['rendezvous'][0]['child_identity']['process_start_ticks'] += 1
    elif damage == 'late_release':
        probe['rendezvous'][0]['release_monotonic_ns'] = parent['phases'][0]['closed_monotonic_ns'] + 1


@pytest.mark.parametrize('damage', [None, 'no_rendezvous', 'sequential_child', 'stale_snapshot', 'wrong_child', 'late_release', 'invalid_fork_collision'])
def test_public_fork_evidence_requires_kernel_bound_concurrent_rendezvous(tmp_path, damage):
    from r2f.campaign.participant_identity import rebuild_participant_identity
    _, capsule = identity_scenario('fork_before_participant')
    config = _replay_config(capsule, 'offline-fork', 80, SOURCE)
    runtime = run_pair(config, tmp_path, topology_backend=IdentityFixture(damage))
    facts = TopologyFactProducer().produce({'events': runtime['events'], 'runtime': runtime, 'config': config})
    assert facts['diagnostic_valid'], facts.get('evidence_error')
    identity = facts['participant_identity']
    assert identity['status'] == ('consistent' if damage is None else 'unknown'), identity
    if damage == 'invalid_fork_collision':
        assert identity['conflicts'] == []
    assert rebuild_participant_identity(runtime['events'], config['topology_program'], config,
        runtime['raw_path'], runtime['raw_sha256']) == identity


@pytest.mark.parametrize('after,status', [(None, 'unknown'), ('fresh_iid', 'consistent'), ('reused_iid', 'conflict')])
def test_raw_review_requires_each_actual_dds_incarnation_and_checks_iid_reuse(tmp_path, after, status):
    from test_endpoint_incarnation import trace, OLD, NEW
    from r2f.campaign.participant_identity import rebuild_participant_identity
    _, capsule = identity_scenario()
    config = _replay_config(capsule, 'offline-incarnation', 80, SOURCE)
    runtime = run_pair(config, tmp_path, topology_backend=IdentityFixture())
    events = deepcopy(runtime['events'])
    origin = next(e for e in events if e['event'] == 'endpoint_created' and e['logical_id'] == 'r0')
    origin.update(node_name='reader_node', rmw_identifier='rmw_cyclonedds_cpp')
    prior = next(e for e in events if e['event'] == 'participant_ownership_snapshot' and e['logical_id'] == 'r0')
    base = max(e['monotonic_ns'] for e in events) + 100
    transitions = json.loads(json.dumps(trace()[1:]).replace(OLD, origin['endpoint_gid']))
    for row in transitions:
        for key in ('logical_id', 'generation', 'pid', 'pgid', 'process_start_ticks', 'endpoint_generation',
                    'run_id', 'domain_id', 'node_name', 'rmw_identifier', 'monotonic_clock_identity', 'role'):
            row[key] = origin[key]
        row['worker_role'] = 'reader'
        if 'topic' in row:
            row['topic'] = origin['topic']
        for key in ('monotonic_ns', 'api_monotonic_ns', 'update_started_ns', 'update_finished_ns'):
            if key in row:
                row[key] += base - 100
    events += transitions
    if after:
        observed = deepcopy(prior)
        observed.update(endpoint_gid=NEW, incarnation_ordinal=1, monotonic_ns=base + 20)
        value = observed['participant_ownership_observation']
        value['local_gid'] = NEW
        for r in value['entity_identities']:
            if r['handle'] == value['local_handle']:
                r['gid'] = NEW
        for sample in value['instance_identity']['samples']:
            sample[1]['guid'] = NEW
            if after == 'fresh_iid':
                sample[1]['instance_id'] += 1000
        events.append(observed)
    for i, event in enumerate(events):
        event['event_index'] = i
    # Reopen the serialized evidence, as the common candidate/replay boundary does.
    path = tmp_path / 'incarnations.raw.jsonl'
    path.write_text(''.join(json.dumps(e) + '\n' for e in events))
    import hashlib
    identity = rebuild_participant_identity([json.loads(line) for line in path.read_text().splitlines()],
        config['topology_program'], config, str(path), hashlib.sha256(path.read_bytes()).hexdigest())
    assert identity['status'] == status, identity
    if after == 'reused_iid':
        assert any(c['kind'] == 'iid_collision' and
            {c[k]['subject'].get('incarnation_ordinal') for k in ('first', 'second')} == {0, 1}
            for c in identity['conflicts'])


def test_three_subject_collision_review_is_independent_of_kernel_pid_order(tmp_path):
    from itertools import combinations
    from r2f.campaign.participant_identity import rebuild_participant_identity
    _, capsule = identity_scenario(readers=2)
    config = _replay_config(capsule, 'offline-three-subjects', 80, SOURCE)
    runtime = run_pair(config, tmp_path, topology_backend=IdentityFixture('collision'))
    events = runtime['events']
    def logical_pairs(rows):
        identity = rebuild_participant_identity(rows, config['topology_program'], config,
            runtime['raw_path'], runtime['raw_sha256'])
        return {tuple(sorted(tuple((v['logical_id'], v['generation']) for v in c[side]['subject']['owner_endpoints'])
            for side in ('first', 'second'))) for c in identity['conflicts'] if c['kind'] == 'guid_collision'}
    pids = sorted({e['pid'] for e in events if e['event'] == 'endpoint_created'})
    mapping = dict(zip(pids, reversed(pids)))
    def permute(value):
        if isinstance(value, dict):
            return {k: mapping.get(v, v) if k in ('pid', 'pgid', 'ppid') else permute(v) for k, v in value.items()}
        if isinstance(value, list):
            return [permute(v) for v in value]
        return value
    expected = set(combinations([(('r0', 1),), (('r1', 1),), (('w0', 1),)], 2))
    assert logical_pairs(events) == logical_pairs(permute(events)) == expected
