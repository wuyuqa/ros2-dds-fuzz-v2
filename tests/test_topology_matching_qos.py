"""Matching carrier input contracts: fixtures do not qualify a DDS execution."""
from r2f.modules.s1 import topology_model as model
from r2f.runtime.topology_pair import build_input_receipts

REFERENCE = {"path": "/offline/native-matching-qos.json", "sha256": "a" * 64}


def test_native_partition_request_remains_visible_at_the_input_boundary():
    program = model.generate_program(7, writers=1, readers=1)
    for endpoint in program["endpoints"]:
        endpoint["qos"]["partitions"] = ["research/alpha"]
    receipt = build_input_receipts(program, {
        "rmw": "rmw_fastrtps_cpp", "native_matching_qos": REFERENCE})
    for endpoint in receipt["endpoints"]:
        assert endpoint["binding_status"] == "expressible", endpoint
        assert endpoint["binding"]["native_matching_qos"]["partitions"] == ["research/alpha"]
        assert endpoint["binding"]["native_matching_qos"]["adoption_status"] == "not_observed"
    assert receipt["execution_evidence"] is False

from copy import deepcopy
import pytest
from test_topology_no_obligation_facts import ObservedPeers, consume, config_for, rewrite
from r2f.core.evidence import INPUT_SCHEMA_V2, effective_input_hash
from r2f.runtime.topology_route import run_topology_route


def produce(tmp_path, *, program, backend):
    config = config_for(program)
    config['rmw'] = config['effective_input']['rmw'] = 'rmw_fastrtps_cpp'
    config['effective_input_sha256'] = effective_input_hash(config['effective_input'], schema=INPUT_SCHEMA_V2)
    config['native_matching_qos'] = deepcopy(REFERENCE)
    result = run_topology_route(config, tmp_path / 'runtime', backend=backend)
    assert result['status'] == 'executed', result
    return result, config


def partition_program():
    program = model.generate_program(7, writers=1, readers=1)
    for endpoint in program['endpoints']:
        endpoint['qos']['partitions'] = ['alpha'] if endpoint['role'] == 'writer' else ['beta']
    return program


def add_native_readbacks(runtime):
    def changed(events):
        for row in events:
            if row['event'] not in ('endpoint_created', 'graph_snapshot'):
                continue
            names = ['alpha'] if row['logical_id'] == 'w0' else ['beta']
            row['native_partition_qos'] = {
                'schema': 'r2f.native-matching-qos/v1', 'status': 'observed',
                'api': 'FastDDS_Publisher_Subscriber_get_qos_set_qos',
                'requested': names, 'before': [], 'after': names, 'return_code': 0, 'ownership': 'shared',
                'endpoint_gid': row['endpoint_gid'], 'pid': row['pid'],
                'sampled_monotonic_ns': row['monotonic_ns'],
                'build_reference': deepcopy(REFERENCE),
                'library': {'path': '/offline/libmatching_qos.so', 'sha256': 'b' * 64},
                'ownership_profile': {'path': '/offline/profile.xml', 'sha256': 'c' * 64},
                'sut_modified': False, 'candidate_eligible': False}
    rewrite(runtime, changed)


def test_expected_partition_refusal_uses_raw_readbacks_and_preserves_s1_attribution(tmp_path):
    runtime, config = produce(tmp_path, program=partition_program(), backend=ObservedPeers(auto_match=False))
    add_native_readbacks(runtime)
    facts, oracle = consume(runtime, config)
    pair = facts['pairs'][0]
    assert pair['compatibility'] == 'incompatible', pair
    assert pair['observed_match'] == 'none'
    policy = pair['proofs']['compatibility']['detail']['policy_scope']
    assert policy['partition']['writer'] == ['alpha']
    assert policy['partition']['reader'] == ['beta']
    assert policy['responsibility_layer'] == 'qos_partition_matching'
    assert policy['s1_coverage_rows'] == ['C10', 'C13', 'C15']
    assert oracle['verdict'] == 'pass', oracle
    assert not oracle['candidate_eligible']


@pytest.mark.parametrize('field,value', [
    ('endpoint_gid', 'f' * 32), ('pid', -1), ('return_code', 3),
    ('sampled_monotonic_ns', 2**63), ('after', ['forged']),
    ('library', None), ('sut_modified', True),
])
def test_bad_native_configuration_is_unknown_and_never_a_sut_failure(tmp_path, field, value):
    runtime, config = produce(tmp_path, program=partition_program(), backend=ObservedPeers(auto_match=False))
    add_native_readbacks(runtime)
    def corrupt(events):
        created = next(e for e in events if e['event'] == 'endpoint_created')
        created['native_partition_qos'][field] = value
    rewrite(runtime, corrupt)
    facts, oracle = consume(runtime, config)
    assert facts['pairs'][0]['compatibility'] == 'unknown'
    assert oracle['verdict'] == 'unknown', oracle
    assert not oracle['candidate_eligible']


def test_a_partition_mismatch_with_observed_communication_is_a_residual(tmp_path):
    runtime, config = produce(tmp_path, program=partition_program(), backend=ObservedPeers())
    add_native_readbacks(runtime)
    facts, oracle = consume(runtime, config)
    assert facts['pairs'][0]['compatibility'] == 'incompatible'
    assert oracle['verdict'] == 'fail', oracle
    assert 'topology.pair_graph_mismatch' in oracle['forbidden_residuals']
    assert not oracle['candidate_eligible']  # explicitly offline fixture


@pytest.mark.parametrize('writer,reader,expected', [
    (['alpha'], ['alpha'], 'compatible'),
    ([], [''], 'compatible'),
    (['zone/*'], ['zone/a'], 'compatible'),
    (['zone/?'], ['zone/ab'], 'incompatible'),
    (['zone/[ab]'], ['zone/a'], 'compatible'),
    (['zone/[ab]'], ['zone/z'], 'incompatible'),
    (['a', 'b'], ['z', 'b'], 'compatible'),
    (['Zone'], ['zone'], 'incompatible'),
])
def test_observed_partition_contract_covers_normal_names_and_posix_patterns(tmp_path, writer, reader, expected):
    program = partition_program()
    for endpoint in program['endpoints']:
        endpoint['qos']['partitions'] = writer if endpoint['role'] == 'writer' else reader
    runtime, config = produce(tmp_path, program=program,
        backend=ObservedPeers(auto_match=expected == 'compatible'))
    add_native_readbacks(runtime)
    def observed(events):
        for row in events:
            if 'native_partition_qos' in row:
                names = writer if row['logical_id'] == 'w0' else reader
                row['native_partition_qos'].update(requested=names, after=names)
    rewrite(runtime, observed)
    facts, oracle = consume(runtime, config)
    assert facts['pairs'][0]['compatibility'] == expected
    assert oracle['verdict'] == 'pass', oracle


def test_missing_fresh_native_partition_readback_is_unknown(tmp_path):
    runtime, config = produce(tmp_path, program=partition_program(), backend=ObservedPeers(auto_match=False))
    add_native_readbacks(runtime)
    def remove(events):
        for row in events:
            if row['event'] == 'graph_snapshot':
                row.pop('native_partition_qos', None)
    rewrite(runtime, remove)
    facts, oracle = consume(runtime, config)
    assert facts['pairs'][0]['compatibility'] == 'unknown'
    assert oracle['verdict'] == 'unknown'


def test_supported_native_ownership_and_public_keep_all_are_not_dropped():
    program = model.generate_program(7, writers=1, readers=1)
    for endpoint in program['endpoints']:
        endpoint['qos'].update(ownership='exclusive', history='keep_all')
    receipt = build_input_receipts(program, {'rmw': 'rmw_fastrtps_cpp', 'native_matching_qos': REFERENCE})
    for endpoint in receipt['endpoints']:
        assert endpoint['binding_status'] == 'expressible', endpoint
        assert endpoint['binding']['native_matching_qos']['ownership'] == 'exclusive'
        assert endpoint['binding']['public_api_qos']['history'] == 'keep_all'


@pytest.mark.parametrize('writer,reader,expected', [
    ('shared', 'shared', 'compatible'), ('exclusive', 'exclusive', 'compatible'),
    ('exclusive', 'shared', 'incompatible'), ('shared', 'exclusive', 'incompatible'),
])
def test_native_ownership_normal_match_and_refusal(tmp_path, writer, reader, expected):
    program = model.generate_program(7, writers=1, readers=1)
    for endpoint in program['endpoints']:
        endpoint['qos']['ownership'] = writer if endpoint['role'] == 'writer' else reader
    runtime, config = produce(tmp_path, program=program,
        backend=ObservedPeers(auto_match=expected == 'compatible'))
    add_native_readbacks(runtime)
    def observed(events):
        for row in events:
            if 'native_partition_qos' in row:
                ownership = writer if row['logical_id'] == 'w0' else reader
                row['native_partition_qos'].update(requested=[], after=[], ownership=ownership)
    rewrite(runtime, observed)
    facts, oracle = consume(runtime, config)
    assert facts['pairs'][0]['compatibility'] == expected
    assert oracle['verdict'] == 'pass', oracle
    assert len(facts['matching_boundary']['pairs']) == 1
    assert facts['matching_boundary']['cross_module_obligations'] == [
        'qos_policy', 'type_contract', 'transport_delivery', 'local_lifecycle']


def test_canonical_planning_persists_the_selected_carrier_without_claiming_execution(tmp_path):
    import json
    from pathlib import Path
    from r2f.campaign.corpus import topology_corpus_from_profile
    from r2f.campaign.feedback import FeedbackLog
    from r2f.campaign.scheduler import ExploratoryScheduler
    from r2f.campaign.runner import run_exploratory_cycle
    from r2f.modules.s1.input_model import content_hash
    root = Path(__file__).resolve().parents[1]
    program = partition_program()
    seeds = {'schema': 'r2f.s1-seed-corpus/v2', 'entries': [{
        'schema': 'r2f.s1-seed-entry/v1', 'seed_id': 'topology.matching', 'surface_id': 'S1.topology',
        'document': program, 'content_sha256': content_hash(program), 'status': 'authored',
        'expected_disposition': 'expect_pass', 'execution_evidence': False, 'candidate_eligible': False,
        'provenance': {'authored': True, 'derived_from_execution': False, 'purpose': 'offline_matching_contract'}}]}
    profile = json.loads((root / 'config/s1_fuzz/profiles/topology-chains.json').read_text())
    spec = {**profile['seed_specs'][0], 'seed_id': 'topology.matching'}
    profile.update(chain_length=1, operator_whitelist=['queue.depth'], rmws=['rmw_fastrtps_cpp'],
                   seed_specs=[spec], declared_seed_specs_union=[deepcopy(spec)], native_matching_qos=REFERENCE)
    profile['cycles'][0]['seed_specs'] = [deepcopy(spec)]
    corpus = topology_corpus_from_profile(profile, source_corpus=seeds)
    feedback = FeedbackLog()
    scheduler = ExploratoryScheduler(corpus, feedback, rng_seed=20, warmup=0)
    cycle = run_exploratory_cycle(tmp_path / 'plan', corpus=corpus, feedback=feedback,
        scheduler=scheduler, profile=profile, execute=False, repo=root, max_inputs=1)
    row = cycle['rows'][0]
    assert row['execution_stage'] == 'planned', row
    config = json.loads(Path(row['runtime_config_path']).read_text())
    capsule = json.loads(Path(row['replay_path']).read_text())
    assert row['native_matching_qos'] == config['native_matching_qos'] == REFERENCE
    assert 'native_matching_qos' not in config['effective_input']['final_input']
    assert capsule['execution'] is None


def test_native_carrier_declares_endpoint_process_scope_before_group_execution():
    program = model.generate_program(7, writers=1, readers=1)
    program['participant_control'] = {'schema': 'r2f.ros-participant-control/v1',
                                      'mechanism': 'process_context_per_group'}
    for endpoint in program['endpoints']:
        endpoint['participant_id'] = 'shared'
    receipt = build_input_receipts(program, {'rmw': 'rmw_fastrtps_cpp', 'native_matching_qos': REFERENCE})
    assert all(e['binding_status'] == 'not_expressible' for e in receipt['endpoints'])
    assert all(e['reason'] == 'native_matching_qos_endpoint_process_only' for e in receipt['endpoints'])


def test_known_qos_refusal_survives_a_separately_reported_partition_observation_gap(tmp_path):
    program = partition_program()
    runtime, config = produce(tmp_path, program=program, backend=ObservedPeers(auto_match=False))
    add_native_readbacks(runtime)
    def lose_partitions(events):
        for row in events:
            if row['event'] == 'graph_snapshot':
                row.pop('native_partition_qos', None)
                for writer in row['publishers']:
                    writer['qos']['reliability'] = 'best_effort'
    rewrite(runtime, lose_partitions)
    facts, oracle = consume(runtime, config)
    assert facts['pairs'][0]['compatibility'] == 'incompatible'
    scope = facts['pairs'][0]['proofs']['compatibility']['detail']['policy_scope']
    assert scope['predicates']['reliability']['compatible'] is False
    assert scope['observation_gaps']
    assert oracle['verdict'] == 'unknown'
    assert not oracle['candidate_eligible']


@pytest.mark.parametrize("rmw", ["rmw_fastrtps_cpp", "rmw_cyclonedds_cpp"])
def test_public_keep_all_binds_the_requested_history_at_carrier_seam(monkeypatch, rmw):
    import sys
    from types import SimpleNamespace
    from r2f.runtime.topology_worker import _public_qos_kwargs
    program = model.generate_program(7, writers=1, readers=1)
    for endpoint in program['endpoints']:
        endpoint['qos']['history'] = 'keep_all'
    receipt = build_input_receipts(program, {'rmw': rmw})
    for endpoint in receipt['endpoints']:
        assert endpoint['binding_status'] == 'expressible', endpoint
    policies = SimpleNamespace(
        HistoryPolicy=SimpleNamespace(KEEP_LAST=1, KEEP_ALL=2),
        ReliabilityPolicy=SimpleNamespace(RELIABLE=1, BEST_EFFORT=2),
        DurabilityPolicy=SimpleNamespace(VOLATILE=1, TRANSIENT_LOCAL=2),
        LivelinessPolicy=SimpleNamespace(AUTOMATIC=1, MANUAL_BY_TOPIC=2))
    monkeypatch.setitem(sys.modules, 'rclpy.qos', policies)
    monkeypatch.setitem(sys.modules, 'rclpy.duration', SimpleNamespace(Duration=lambda **kw: kw))
    for endpoint in receipt['endpoints']:
        assert _public_qos_kwargs(endpoint['binding']['public_api_qos'])['history'] == 2


def test_published_matching_capabilities_identify_native_scope_and_public_history():
    from r2f.runtime.topology_pair import topology_capabilities
    fields = topology_capabilities()['supported_bypass_fields']
    assert fields['history_keep_all'] is True
    assert fields['ownership_non_shared']['carrier'] == 'native_matching_qos'
    assert fields['ownership_non_shared']['scope'] == 'endpoint_process'
    assert fields['partitions_non_default']['scope'] == 'endpoint_process'


@pytest.mark.parametrize('malformed', ['invalid-observed-qos', ['invalid'], 1])
def test_malformed_observed_qos_returns_unknown_instead_of_crashing(tmp_path, malformed):
    program = partition_program()
    runtime, config = produce(tmp_path, program=program, backend=ObservedPeers(auto_match=True))
    add_native_readbacks(runtime)
    def damage(events):
        for row in events:
            if row['event'] == 'graph_snapshot':
                for member in row['publishers']:
                    member['qos'] = deepcopy(malformed)
    rewrite(runtime, damage)
    facts, oracle = consume(runtime, config)
    assert facts['pairs'][0]['compatibility'] == 'unknown'
    assert oracle['verdict'] == 'unknown'


@pytest.mark.parametrize('mismatch', ['topic', 'type'])
def test_independent_refusal_retains_native_freshness_gap(tmp_path, mismatch):
    program = partition_program()
    reader = next(e for e in program['endpoints'] if e['role'] == 'reader')
    reader['topic' if mismatch == 'topic' else 'type_name'] = (
        '/other_topic' if mismatch == 'topic' else 'std_msgs/msg/UInt8MultiArray')
    runtime, config = produce(tmp_path, program=program, backend=ObservedPeers(auto_match=False))
    add_native_readbacks(runtime)
    def remove(events):
        for row in events:
            if row['event'] == 'graph_snapshot':
                row.pop('native_partition_qos', None)
    rewrite(runtime, remove)
    facts, oracle = consume(runtime, config)
    assert facts['pairs'][0]['compatibility'] == 'incompatible'
    assert facts['matching_observation_incomplete'] is True
    assert oracle['verdict'] == 'unknown'
    assert facts['candidate_eligible'] is False


@pytest.mark.parametrize('damage',[None,'missing_selection','bad_native_identity'])
def test_public_application_audit_retains_native_carrier_selection(tmp_path,damage):
    """Synthetic public-schema raw reuses the real route auditor, no live credit."""
    from r2f.core.mutation_plan import MutationPlan
    from r2f.runtime.topology_route import audit_topology_application
    from r2f.runtime.topology_worker import endpoint_expressibility
    from r2f.runtime.qos_binding import compare_public_qos
    runtime,cfg=produce(tmp_path,program=partition_program(),backend=ObservedPeers(auto_match=False))
    add_native_readbacks(runtime)
    specs={e['logical_id']:e for e in cfg['topology_program']['endpoints']}
    def public_schema(events):
        for token in events:
            if token['event']!='endpoint_created':continue
            q=endpoint_expressibility(specs[token['logical_id']],cfg)['public_qos']
            token.update(backend='ros2_public_api',real_ros=True,rmw_identifier='rmw_fastrtps_cpp',
                configuration_source='public_endpoint_actual_qos',qos=deepcopy(q),requested_qos=deepcopy(q),
                identity_source='get_publishers_info_by_topic' if token['role']=='writer' else 'get_subscriptions_info_by_topic',
                created_monotonic_ns=token['monotonic_ns']-2,
                actual_endpoint_qos={'schema':'r2f.public-endpoint-qos/v1','status':'observed',
                    'api':'rcl_publisher_get_actual_qos' if token['role']=='writer' else 'rcl_subscription_get_actual_qos',
                    'qos':deepcopy(q),'pid':token['pid'],'sampled_monotonic_ns':token['monotonic_ns']-1,
                    'library':{'path':'/fixture/librcl.so','sha256':'a'*64},'abi':{'verified':True,'fixture':True}},
                qos_request_comparison=compare_public_qos(q,q))
            if damage=='missing_selection':token.pop('native_partition_qos')
            elif damage=='bad_native_identity':token['native_partition_qos']['pid']=-1
    rewrite(runtime,public_schema)
    audit=audit_topology_application(MutationPlan.from_dict(cfg['mutation_plan']),cfg['input_layer_receipts'],runtime)
    if damage is None:
        assert audit['status']!='invalid',audit
        assert all(not layer['reasons'] for layer in audit['layers']),audit
    else:
        assert audit['status'] in ('invalid','incomplete'),audit
        assert not audit['mutation_receipt']['hit']
    assert audit['execution_evidence'] is False


@pytest.mark.parametrize('ownership,expected', [('shared','SHARED'), ('exclusive','EXCLUSIVE')])
def test_ownership_profile_uses_pinned_fastdds_xml_grammar(ownership,expected):
    import xml.etree.ElementTree as ET
    from r2f.runtime.matching_qos import ownership_profile
    root=ET.fromstring(ownership_profile('/r2f/test',ownership))
    ns={'d':'http://www.eprosima.com/XMLSchemas/fastRTPS_Profiles'}
    assert [node.text for node in root.findall('.//d:ownership/d:kind',ns)]==[expected,expected]
