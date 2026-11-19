"""Exact a938 route pairs; pure-input tests, never runtime execution.

Original task/RNG bindings were reconstructed before the v4 change. Fixed RNGs
below replay those inputs, NOT tasks regenerated from a changed v4 root.
"""
from copy import deepcopy
import json
from pathlib import Path

import pytest

from r2f.campaign.corpus import Corpus, content_identity, content_sha256, corpus_from_profile
from r2f.campaign.search_frontier import InputSearchFrontier, derived_record
from r2f.core.evidence import canonical_hash, effective_input_hash, INPUT_SCHEMA_V2
from r2f.core.mutation_plan import RUNTIME_WINDOW_SCHEMA
from r2f.modules.s1.topology_input import make_topology_plan

PROFILE = Path(__file__).resolve().parents[1] / 'config/s1_fuzz/profiles/model-bfs-topology-runtime-smoke.json'

ORIGINAL = {'17': {'requested': {'operator_id': 'generation.action',
                      'slot_id': 'S1.topology.actions',
                      'context': {'variant': 'next'}},
        'rng_seed': 5319083829430445513,
        'task_id': '89479bbd8429d328e8661528f23eb8b3e80d12659f8ad6a3ab21573ba18c2d7c',
        'content_sha256': '318d59b7483f5af272b359f2990257f51ccdf4d969c76ce4ac0feb4849ff3d0c',
        'final_config_canonical_sha256': '385de77d449cd809b367fff59a6f59db54ef62abbe1e0db97b42a1aae369b0a4'},
 '16': {'requested': {'operator_id': 'generation.action',
                      'slot_id': 'S1.topology.generation',
                      'context': {'variant': 'next'}},
        'rng_seed': 6056410188658595942,
        'task_id': '4db0511e54d6252d81c1345f75d3a25ffc57954899688baa4fcd93f0fd80ff8e',
        'content_sha256': 'f21b1b4ea383e0e31746e86484cbc4c20bc4e8f9482da0b37fb6e0281638b006',
        'final_config_canonical_sha256': '385de77d449cd809b367fff59a6f59db54ef62abbe1e0db97b42a1aae369b0a4'},
 '26': {'requested': {'operator_id': 'guard.dependency', 'slot_id': 'S1.topology.actions', 'context': {}},
        'rng_seed': 4784778232090518704,
        'task_id': 'dc65e31825309cd38d199f6a02a57bd461bf5caa06ac41ab9c4eb2da5a1857f5',
        'content_sha256': '0d3d43360fa4380d556d68d6ec7701d777ab063e87e12d367cde3b787a8ac31d',
        'final_config_canonical_sha256': '7073886612eb4908b78aa7d24025ba43cb937862b4d4081c3d884e47025b6860'},
 '27': {'requested': {'operator_id': 'guard.dependency', 'slot_id': 'S1.topology.dependency', 'context': {}},
        'rng_seed': 6755353433637513382,
        'task_id': 'b64c25c3a84a1f2f6d0ae652df2f4a08f79be928869b44dcdd9429b0fbba8ce3',
        'content_sha256': '58019b82c9872d2a88fb3b0ca0c06f3c75fa1c20d3bb3b42fe8a07f04854e819',
        'final_config_canonical_sha256': '7073886612eb4908b78aa7d24025ba43cb937862b4d4081c3d884e47025b6860'},
 '32': {'requested': {'operator_id': 'env.lease_refresh', 'slot_id': 'S1.topology.qos.lease', 'context': {}},
        'rng_seed': 9795163566921196674,
        'task_id': 'ac2010940e7b2d649d662d3ab4c029e0513a8479fe785addbac926abd6c27748',
        'content_sha256': '59d0eb16605888eae8c4319dfd2480bf130d22bf496fb197c1997cb906b9d9b1',
        'final_config_canonical_sha256': '5b964219f9f83e2d878b95136d4315fe56057b041c534b063a6d2b5bc0f12180'},
 '56': {'requested': {'operator_id': 'param.boundary',
                      'slot_id': 'S1.topology.qos.lease',
                      'context': {'boundary_class': 'zero'}},
        'rng_seed': 15170956880995094200,
        'task_id': '093b43c1048dbfe5c66bec1a6105ebdc2fa8756062826018b0045eb18acb7c0e',
        'content_sha256': '57b0d66cb001741f06709db7c9102b2f64a31081fedc54c11bd1ca979b95cdb0',
        'final_config_canonical_sha256': '5b964219f9f83e2d878b95136d4315fe56057b041c534b063a6d2b5bc0f12180'},
 '38': {'requested': {'operator_id': 'env.resource',
                      'slot_id': 'S1.topology.environment',
                      'context': {'key': 'backend_available'}},
        'rng_seed': 7858636182185058896,
        'task_id': 'cf0ff28d422647a4a5e14df97c5a60697bc81e76baa781b1ea4f95d308ba0a4c',
        'content_sha256': '42890f13acd33e4e8afd39aa3bdc93c538d3870edbac5129123255855bf73278',
        'final_config_canonical_sha256': '1b3a996a8cb58a850aeaaa2db908bd5ebf443d58c0d7d14d60aa0cbd5c73547c'},
 '43': {'requested': {'operator_id': 'guard.environment',
                      'slot_id': 'S1.topology.environment',
                      'context': {'key': 'backend_available'}},
        'rng_seed': 12603776071744823804,
        'task_id': 'ab796628fb0478f4dad0e448e1f83a20c32acdf5ab393656299646e11b571f07',
        'content_sha256': '7f2a08e1028f4c74dac2021b8f39f3cc57c0c4ff111df72e84625c82492f33ea',
        'final_config_canonical_sha256': '1b3a996a8cb58a850aeaaa2db908bd5ebf443d58c0d7d14d60aa0cbd5c73547c'},
 '39': {'requested': {'operator_id': 'env.resource',
                      'slot_id': 'S1.topology.environment',
                      'context': {'key': 'transport_ready'}},
        'rng_seed': 14022050954454261196,
        'task_id': 'd6f078d2b7f547c1ba91e34d4bd3048a9cf4714138eee45b7c7e5c6203e33673',
        'content_sha256': '925660101b5ef2207644934018feb3883a36a31d7055bba010ddb7ad933c99dd',
        'final_config_canonical_sha256': 'fca06bfa054680209451f2959020c400219c69d6beb91d8eaa3a2d5cf1acb344'},
 '44': {'requested': {'operator_id': 'guard.environment',
                      'slot_id': 'S1.topology.environment',
                      'context': {'key': 'transport_ready'}},
        'rng_seed': 3970759771811925237,
        'task_id': '500422151e2cad60d0162ada4bb9f7fbe6c52630743583a8f57966e2d30da804',
        'content_sha256': 'b2a50b8638c0a5fe435e533eccc356abf795b0101ef541a34d138a93099b6bf5',
        'final_config_canonical_sha256': 'fca06bfa054680209451f2959020c400219c69d6beb91d8eaa3a2d5cf1acb344'},
 '40': {'requested': {'operator_id': 'env.resource',
                      'slot_id': 'S1.topology.environment',
                      'context': {'key': 'discovery_enabled'}},
        'rng_seed': 15381913848352663032,
        'task_id': 'd9c69b838dec26370d646db8d557d4c8b82e010ca98275a567518c9bce4c1e8f',
        'content_sha256': '311fbdb46af40b21058277a76eb3fbf89bc0aed08d9c77d32cd9b9944d6c1086',
        'final_config_canonical_sha256': '147dd502dbb1de58582d32ac8f5217b252faa878fbfeacb01e984cb204fe7273'},
 '45': {'requested': {'operator_id': 'guard.environment',
                      'slot_id': 'S1.topology.environment',
                      'context': {'key': 'discovery_enabled'}},
        'rng_seed': 16344917687080184869,
        'task_id': '75603ba59a56d1098f0fa0bbde907c15abbdf10b19bf7d1e16faed17c0d868ea',
        'content_sha256': '7c9c8b90c918c933db406d2143e1b4e8272e09217b7a4bb134a2e4831d6d09d6',
        'final_config_canonical_sha256': '147dd502dbb1de58582d32ac8f5217b252faa878fbfeacb01e984cb204fe7273'},
 '41': {'requested': {'operator_id': 'env.resource',
                      'slot_id': 'S1.topology.environment',
                      'context': {'key': 'filter_support'}},
        'rng_seed': 12954601797667590691,
        'task_id': '9032446a81d995a74ebd6f2ca33585385f6936de9c8cffcb70a394b5323850d3',
        'content_sha256': 'ce73c69e7beb4591351a78990e0fedfd26be3249aa2b7739017f4cb742f042b3',
        'final_config_canonical_sha256': 'b35dae6bc69afbef913a492edce25ed05df8ee4306335858947c6f8235d642b8'},
 '46': {'requested': {'operator_id': 'guard.environment',
                      'slot_id': 'S1.topology.environment',
                      'context': {'key': 'filter_support'}},
        'rng_seed': 2376796673358513545,
        'task_id': 'd1f953f81dfe86e6b8b5524ffdbab7a93b3eb7a3c701e84a887b5d27eb5a5516',
        'content_sha256': '5dfd8e65681c1209d67fe1d2d7849da8085ffa9cb283f6ca022863037842ddeb',
        'final_config_canonical_sha256': 'b35dae6bc69afbef913a492edce25ed05df8ee4306335858947c6f8235d642b8'},
 '42': {'requested': {'operator_id': 'env.resource',
                      'slot_id': 'S1.topology.environment',
                      'context': {'key': 'filter_validity'}},
        'rng_seed': 17935205424284704179,
        'task_id': '77adc2e84871677c2de7ebaf81c657b24581c6a7ee83a54f0b083868d9ac7acf',
        'content_sha256': '085a428ac41119c34a685a218dd21543ccff2f2414dba5e9b52455b62fc11023',
        'final_config_canonical_sha256': '5fc25e7dd9535f2c2c947f983f7eb45715cb7099c52b991a93bb4d9f90e88922'},
 '47': {'requested': {'operator_id': 'guard.environment',
                      'slot_id': 'S1.topology.environment',
                      'context': {'key': 'filter_validity'}},
        'rng_seed': 128411073875160610,
        'task_id': '8c4e2a7b196451d48c17eb73d43c4ee47dc07c1171998964f367750562d16bf5',
        'content_sha256': 'e7c5004319059ba71f35dea63355607b307782f9977e96eb48731064287fc316',
        'final_config_canonical_sha256': '5fc25e7dd9535f2c2c947f983f7eb45715cb7099c52b991a93bb4d9f90e88922'}}
PAIRS = [('17', '16'), ('26', '27'), ('32', '56'), ('38', '43'), ('39', '44'), ('40', '45'), ('41', '46'), ('42', '47')]


@pytest.fixture(scope='module')
def parent():
    return next(iter(corpus_from_profile(PROFILE, cycle_id='topology-authored').entries.values()))


@pytest.fixture(scope='module')
def members(parent):
    result = {}
    for index, original in ORIGINAL.items():
        plan = make_topology_plan(parent['final_config']['topology_program'],
                                  [original['requested']], rng_seed=original['rng_seed'])
        result[index] = derived_record(parent, plan)
        assert canonical_hash(result[index]['final_config']) == original['final_config_canonical_sha256']
    return result


def detached(record):
    record = deepcopy(record)
    record.pop('mutation_plan')
    record.pop('content_sha256')
    return record


def test_group1_exact_routes_share_content_but_not_lineage_or_effective_input(members):
    a, b = (members[i] for i in PAIRS[0])
    saved = deepcopy((a, b))
    assert a['final_config'] == b['final_config']
    assert a['runtime_windows'] == b['runtime_windows']
    assert a['content_sha256'] == b['content_sha256']
    assert canonical_hash(a['mutation_plan']) != canonical_hash(b['mutation_plan'])
    from test_fuzz_identity import legacy_input
    def effective(record):
        data = legacy_input()
        data.update({k: record[k] for k in ('target_id', 'target_revision', 'rmw', 'contract_hash',
                                          'mutation_plan', 'operations', 'runtime_windows')})
        data.update(input_identity_schema=INPUT_SCHEMA_V2, final_input=record['final_config'])
        return effective_input_hash(data)
    assert effective(a) != effective(b)
    assert saved == (a, b)


@pytest.mark.parametrize('pair', PAIRS[1:], ids=['group2','group3','group4','group5','group6','group7','group8'])
def test_other_exact_groups_remain_separate_pending_execution_key_layering(members, pair):
    a, b = (members[i] for i in pair)
    assert a['final_config'] == b['final_config']
    assert a['content_sha256'] != b['content_sha256']


@pytest.mark.parametrize('index', ['26', '27', '38', '43'])
def test_guard_terminal_members_are_not_sut_executions(members, index):
    from r2f.core import Registry
    from r2f.core.scenario import Scenario
    from r2f.core.mutation_plan import MutationPlan
    from r2f.modules.s1.module import register_exploratory
    record = members[index]
    assert record['executable'] is False
    assert record['candidate_eligible'] is False
    registry = Registry()
    register_exploratory(registry, include_required=False, runtime_capability='topology_v1')
    target = registry.get_target(record['target_id'])
    plan = MutationPlan.from_dict(record['mutation_plan'])
    scenario = Scenario(target, record['rmw'], 'mutated', 0, ORIGINAL[index]['rng_seed'],
                        {'mutation_kind': 'endpoint_program', 'carrier': 'ros2_endpoint',
                         'mutation_parameters': plan.final_config}, plan)
    with pytest.raises(ValueError):
        registry.get_services(target.target_id).mutation_provider.prepare_plan(scenario, plan)


@pytest.mark.parametrize('change', ['endpoint', 'generation', 'action', 'order', 'timing', 'environment',
                                  'guards', 'preconditions', 'unknown_payload', 'contract', 'rmw', 'revision'])
def test_actual_execution_inputs_and_audit_obligations_stay_distinct(members, change):
    a = detached(members['17']); b = deepcopy(a)
    program = b['final_config']['topology_program']
    action = program['actions'][-1]
    if change == 'endpoint': action['endpoint_id'] = 'w1'
    elif change == 'generation': action['generation'] += 1
    elif change == 'action': action['op'] = 'remove'
    elif change == 'order': program['actions'].reverse()
    elif change == 'timing': action['params']['delay_sec'] = .125
    elif change == 'environment': program.setdefault('environment', {})['transport_ready'] = False
    elif change == 'guards': program['guards'] = [{'guard': 'unknown', 'value': False}]
    elif change == 'preconditions': program['preconditions'] = ['unknown']
    elif change == 'unknown_payload': program['payloadkeys'] = {'seed': 'not-a-selector'}
    elif change == 'contract': b['contract_hash'] = 'a' * 64
    elif change == 'rmw': b['rmw'] = 'rmw_fastrtps_cpp'
    elif change == 'revision': b['target_revision'] += 1
    assert content_identity(a) != content_identity(b)


def test_real_runtime_windows_and_operation_order_stay_distinct(members):
    a = detached(members['17'])
    window = {'schema': RUNTIME_WINDOW_SCHEMA, 'window_id': 'w1', 'window_index': 0,
              'trigger_anchor': 'fresh_match', 'trigger_offset_sec': 0,
              'drop_duration_sec': .1, 'restore_interval_sec': .2}
    a['runtime_windows'] = [window, dict(window, window_id='w2', window_index=1, drop_duration_sec=.4)]
    b = deepcopy(a); b['runtime_windows'][0]['trigger_offset_sec'] = .25
    assert content_identity(a) != content_identity(b)
    b = deepcopy(a)
    b['runtime_windows'][0]['drop_duration_sec'], b['runtime_windows'][1]['drop_duration_sec'] = .4, .1
    assert content_identity(a) != content_identity(b)
    a['operations'].append(deepcopy(members['39']['operations'][0]))
    a['operations'][1]['op_id'] = 'second'
    b = deepcopy(a); b['operations'].reverse()
    assert content_identity(a) != content_identity(b)


@pytest.mark.parametrize('change', ['runtime', 'surface', 'mutator', 'operator', 'target', 'program_schema',
                                  'extra_envelope', 'unknown_context', 'nested_context', 'stale',
                                  'receipt_disposition', 'status', 'receipt_schema', 'unresolved_slot'])
def test_alias_projection_does_not_widen(members, change):
    a, b = (detached(members[i]) for i in PAIRS[0])
    for record in (a, b):
        op = record['operations'][0]; p = op['parameters']; receipt = p['input_layer_receipts'][0]
        if change == 'runtime': op['application_phase'] = 'runtime_window'
        elif change == 'surface': op['surface_id'] = 'unknown'
        elif change == 'mutator': op['mutator_id'] = 'unknown'
        elif change == 'operator': p['operator_id'] = p['request']['operator_id'] = 'unknown'
        elif change == 'target': record['target_id'] = 'unknown'
        elif change == 'program_schema': record['final_config']['topology_program']['schema'] = 'unknown'
        elif change == 'extra_envelope': p['payloadkeys'] = {'seed': 1}
        elif change == 'unknown_context': p['request']['context']['payloadkeys'] = True
        elif change == 'nested_context': p['request']['context']['parameters'] = {'variant': 'next'}
        elif change == 'stale': p['request']['context']['variant'] = 'stale'
        elif change == 'receipt_disposition': receipt['expected_disposition'] = 'unknown_not_pass'
        elif change == 'status': op['applicability_result']['expected_disposition'] = 'unknown_not_pass'
        elif change == 'receipt_schema': receipt['program_schema'] = 'unknown'
        elif change == 'unresolved_slot' and '#' in p['slot_id']:
            p['slot_id'] = receipt['slot_id'] = 'S1.topology.generation'
    assert content_identity(a) != content_identity(b)


def test_old_v3_derived_and_checkpoint_rejected_without_mutation(members):
    for index in PAIRS[0]:
        record = members[index]
        old = ORIGINAL[index]['content_sha256']
        saved = deepcopy(record)
        with pytest.raises(ValueError, match='content hash mismatch'):
            content_sha256(dict(record, content_sha256=old))
        with pytest.raises(ValueError, match='content hash mismatch'):
            Corpus().add_seed(dict(record, content_sha256=old))
        corpus = Corpus(); corpus.add_seed(record)
        data = corpus.to_dict(); data['entries'][0]['content_sha256'] = old
        checkpoint = deepcopy(data)
        with pytest.raises(ValueError, match='content hash mismatch'):
            Corpus.from_dict(data)
        assert data == checkpoint
        assert record == saved


def test_group1_frontier_attempts_remain_separate_duplicate_content_not_child(parent):
    from r2f.campaign.topology_search import descriptor, generate, SCHEMA
    profile = json.loads(PROFILE.read_text())
    samples = [dict(ORIGINAL[index]['requested'], class_id='next') for index in PAIRS[0]]
    desc = descriptor({'schema': SCHEMA, 'samples': samples}, ['generation.action'])
    frontier = InputSearchFrontier([parent], profile['search'], rng_seed=41,
        sampling_kinds=None, sampling_weight=None, topology_descriptor=desc)
    tasks = [t for t in frontier.tasks if t['derivation_depth'] == 1]
    assert len(tasks) == 2
    records = [derived_record(frontier.entry(task), generate(frontier.entry(task), task, desc))
               for task in tasks]
    assert records[0]['final_config'] == records[1]['final_config']
    tasks[0]['status'] = 'selected'; frontier.planned(tasks[0]['task_id'], records[0])
    before = len(frontier.nodes)
    tasks[1]['status'] = 'selected'; frontier.planned(tasks[1]['task_id'], records[1])
    assert tasks[0]['task_id'] != tasks[1]['task_id']
    assert tasks[0]['generation_outcome'] == 'new_input'
    assert tasks[1]['generation_outcome'] == 'duplicate_content'
    assert len(frontier.nodes) == before
    # These are fresh v4 task/RNG bindings. The historical a938 bindings were
    # independently rebuilt pre-change, and their exact 16 inputs tested above.


def test_real_endpoint_selection_and_same_seed_multi_operator_chain(parent):
    program = parent['final_config']['topology_program']
    request = {'operator_id': 'generation.action', 'slot_id': 'S1.topology.actions',
               'context': {'variant': 'next'}}
    a = derived_record(parent, make_topology_plan(program, [request], rng_seed=41))
    alternate = deepcopy(request); alternate['context']['endpoint_id'] = 'w1'
    b = derived_record(parent, make_topology_plan(program, [alternate], rng_seed=41))
    assert a['final_config'] != b['final_config']
    assert a['content_sha256'] != b['content_sha256']
    relation = {'operator_id': 'relation.pair', 'slot_id': 'S1.topology.qos.reliability',
                'context': {'pair': 'RR'}}
    alternate = deepcopy(request); alternate['slot_id'] = 'S1.topology.generation'
    a, b = [derived_record(parent, make_topology_plan(program, [first, relation], rng_seed=41))
            for first in (request, alternate)]
    assert len(a['operations']) == len(b['operations']) == 2
    assert a['final_config'] == b['final_config']
    assert a['content_sha256'] == b['content_sha256']
    assert canonical_hash(a['mutation_plan']) != canonical_hash(b['mutation_plan'])
