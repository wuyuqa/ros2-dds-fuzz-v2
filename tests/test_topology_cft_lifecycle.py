"""CFT lifecycle via the shared runner; authored fixtures never qualify DDS."""
import json
from pathlib import Path
from unittest.mock import Mock

import pytest

from r2f.runtime.ros2_pair import run_pair
from r2f.runtime.topology_worker import OfflineTopologyBackend
from test_topology_cycle import run_cycle, profile


def test_runner_persists_initial_and_dynamic_filter_observation_gaps(tmp_path):
    doc = profile(1)
    for specs in (doc['seed_specs'], doc['declared_seed_specs_union'], doc['cycles'][0]['seed_specs']):
        specs[0]['seed_id'] = 'topology.cft_matrix'
    runtime = Mock(side_effect=lambda cfg, out: run_pair(
        cfg, out, topology_backend=OfflineTopologyBackend()))
    result = run_cycle(tmp_path / 'lifecycle', doc, runtime=runtime)
    row = result['rows'][0]
    facts = row['typed_facts']
    evidence = facts['content_filter_observations']
    assert evidence['schema'] == 'r2f.cft-sample-observations/v1'
    assert evidence['declared'] is True
    assert evidence['configuration_observations']
    assert evidence['fixture'] is True
    assert evidence['execution_evidence'] is False
    assert all(item['filter_effect_observed'] is None
               for item in evidence['configuration_observations'])
    assert evidence['sample_observations']
    assert all(item['status'] == 'unknown' and item['sample_effect_observed'] is None
               for item in evidence['sample_observations'])
    assert row['candidate_eligible'] is False
    assert Path(row['runtime_result_path']).exists()
    report = json.loads((tmp_path / 'lifecycle' / 'cycle.json').read_text())
    assert report['rows'][0]['typed_facts']['content_filter_observations'] == evidence


def initial_rows(*, real_envelope=False, expression='data = %0', parameters=None):
    """Public creation API double; explicitly NON-DDS and not persisted evidence."""
    from copy import deepcopy
    import time
    from test_topology_content_filter import FakeNode, FakeOptions, FakeSubscription
    from r2f.runtime.topology_worker import _create_reader_with_initial_filter
    rows = []
    lineage = dict(logical_id='r0', generation=1, endpoint_generation='fixture-process',
                   pid=123, pgid=123, process_start_ticks=4, run_id='fixture-initial',
                   domain_id=191, node_name='reader', role='reader',
                   rmw_identifier='offline-fixture', monotonic_clock_identity={'boot_id': 'fixture'},
                   topic='/fixture/cft', message_type='std_msgs/msg/String',
                   backend='offline_fake', fixture=True, real_ros=False)
    if real_envelope:
        lineage.pop('fixture')
        lineage.update(backend='ros2_public_api', real_ros=True)
    def emit(event, **fields):
        row = dict(lineage, event=event, event_index=len(rows), monotonic_ns=time.monotonic_ns())
        row.update(fields)
        rows.append(row)
        return row
    action = dict(action_id='create-reader', op='create', endpoint_id='r0', generation=1,
                  params={}, depends_on=[])
    emit('topology_action_begin', action_id=action['action_id'], action=action)
    create = emit('endpoint_create_begin')
    parameters = ['keep'] if parameters is None else parameters
    _, receipt = _create_reader_with_initial_filter(
        FakeNode(FakeSubscription(expression, parameters, enabled=bool(expression))), object, lineage['topic'], lambda _: None, object,
        {'expression': expression, 'parameters': parameters},
        options_factory=FakeOptions, emit=emit, fixture=not real_envelope)
    emit('endpoint_created', endpoint_gid='01' * 16, initial_content_filter=deepcopy(receipt),
         created_monotonic_ns=receipt['creation_returned_monotonic_ns'],
         create_begin_monotonic_ns=create['monotonic_ns'])
    emit('topology_action_done', action_id=action['action_id'], action=action)
    return rows, action


@pytest.mark.parametrize('real_envelope', [False, True])
def test_initial_public_readback_is_independently_bound_to_creation(real_envelope):
    from r2f.runtime.filter_action_receipt import filter_action_evidence
    rows, action = initial_rows(real_envelope=real_envelope)
    proof = filter_action_evidence(action, rows)
    assert proof['phase'] == 'initial'
    assert proof['observed_enabled'] is True
    assert proof['outcome_kind'] == 'configuration_readback'
    assert proof['observed'] == {'expression': 'data = %0', 'parameters': ['keep']}
    assert proof['filter_effect_observed'] is None
    assert proof['candidate_eligible'] is False


@pytest.mark.parametrize('damage', ['getter', 'state', 'bool_alias', 'identity', 'clock', 'raw_ref', 'before_submitted', 'submitted_creation'])
def test_initial_configuration_cannot_be_promoted_from_partial_or_wrong_evidence(damage):
    from r2f.runtime.filter_action_receipt import filter_action_evidence
    rows, action = initial_rows()
    value = next(e for e in rows if e['event'] == 'content_filter_initial_configuration')['filter_receipt']
    token = next(e for e in rows if e['event'] == 'endpoint_created')
    if damage == 'getter':
        value['api_results']['get_content_filter']['raw_result']['filter_expression'] = 'data = 5'
    elif damage == 'state':
        value['api_results']['is_cft_enabled']['status'] = 'unavailable_api'
    elif damage == 'bool_alias':
        value['api_results']['is_cft_enabled']['raw_result'] = 1
    elif damage == 'identity':
        rows[2]['process_start_ticks'] += 1
    elif damage == 'clock':
        value['api_results']['is_cft_enabled']['completed_monotonic_ns'] = 1
    elif damage == 'before_submitted':
        stamp = value['creation_returned_monotonic_ns']
        for api in value['api_results'].values():
            api.update(started_monotonic_ns=stamp, completed_monotonic_ns=stamp)
        value['monotonic_ns'] = stamp
    elif damage == 'submitted_creation':
        submitted = next(e for e in rows if e['event'] == 'content_filter_initial_submitted')
        submitted['filter_receipt']['creation_returned_monotonic_ns'] += 10**9
    else:
        rows[2]['event_index'] = -1
    # Keep the summary echo consistent to exercise the independent raw gate.
    from copy import deepcopy
    token['initial_content_filter'] = deepcopy(value)
    with pytest.raises(ValueError):
        filter_action_evidence(action, rows)


def test_lifecycle_seed_is_opt_in_and_reaches_the_existing_corpus():
    from r2f.campaign.corpus import topology_corpus_from_profile
    from r2f.modules.s1.fuzz_mutators_v2 import corpus_v2
    doc = profile(1)
    for specs in (doc['seed_specs'], doc['declared_seed_specs_union'], doc['cycles'][0]['seed_specs']):
        specs[0]['seed_id'] = 'topology.cft_lifecycle'
    selected = topology_corpus_from_profile(doc, cycle_id='topology-authored').selectable()
    assert len(selected) == 2  # one authored input per configured RMW
    program = selected[0]['final_config']['topology_program']
    reader = next(e for e in program['endpoints'] if e['logical_id'] == 'r0')
    assert reader['filter']['expression'] == 'data LIKE %0'
    changes = [a for a in program['actions'] if a['op'] == 'update_filter']
    assert [a['action_id'] for a in changes] == ['update_parameters', 'disable', 'enable', 'restore_disabled']
    assert all(e['seed_id'] != 'topology.cft_lifecycle' for e in corpus_v2()['entries'])


def test_complete_string_predicate_expectation_is_independent_of_declared_payload():
    from r2f.campaign.topology_cft_observation import string_filter_expectation
    assert string_filter_expectation('data LIKE %0', ["'%KEEP%'"], '{"data":"DROP"}') is False
    assert string_filter_expectation('data LIKE %0', ["'%KEEP%'"], '{"metadata":"KEEP","declared_payload":"DROP"}') is True
    assert string_filter_expectation('', [], 'anything') is True
    assert string_filter_expectation('data LIKE %0', ["'%K_EP%'"], 'KEEP') is None


def test_CFT_predicate_residual_is_not_hidden_by_other_pair_observations(tmp_path):
    from test_topology_adapter import RawFixture
    from r2f.modules.s1.topology_oracles import TopologyOracle
    fixture = RawFixture()
    facts = fixture.facts(tmp_path)
    facts['content_filter_observations'] = {
        'schema': 'r2f.cft-sample-observations/v1', 'fixture': True,
        'configuration_observations': [], 'sample_observations': [
            {'violation': 'topology.cft_predicate_mismatch', 'status': 'native_accepted_and_delivered',
             'expected_decision': False, 'actual_decision': True, 'sample_effect_observed': True,
             'writer': 'w0', 'writer_generation': 1, 'reader': 'r0', 'reader_generation': 1,
             'sequence': 1, 'action_id': 'enable'}], 'review_required': True}
    result = TopologyOracle().evaluate(fixture.scenario, facts)
    assert result['verdict'] == 'fail'
    assert 'topology.cft_predicate_mismatch' in result['forbidden_residuals']
    assert result['candidate_eligible'] is False  # explicit fixture never qualifies


def authored_sample_contract(*, expression='data LIKE %0', parameters=None, unknown=False):
    """Pure in-memory protocol model; it is never a real run/candidate/seal."""
    from copy import deepcopy
    from test_topology_publish_occurrence import native_window
    from r2f.campaign.topology_sample_attribution import native_delivery_identity
    from r2f.campaign.topology_cft_observation import content_filter_observations
    rows, _, _ = native_window()
    protocol, action = initial_rows(real_envelope=True, expression=expression,
                                   parameters=["'%' ".strip()] if parameters is None else parameters)
    origin = rows[1]
    for event in protocol:
        for key in ('logical_id', 'generation', 'endpoint_generation', 'pid', 'pgid',
                    'process_start_ticks', 'run_id', 'domain_id', 'node_name', 'role',
                    'rmw_identifier', 'monotonic_clock_identity', 'topic', 'message_type'):
            event[key] = origin[key]
    protocol[-2]['endpoint_gid'] = origin['endpoint_gid']
    rows[1:2] = protocol
    for i, event in enumerate(rows):
        event.update(event_index=i, monotonic_ns=10000 + i * 1000)
        if event.get('rcl_take_receipt'):
            event['rcl_take_receipt'].update(take_begin_monotonic_ns=event['monotonic_ns'] - 1,
                                            take_return_monotonic_ns=event['monotonic_ns'])
    submitted = next(e for e in rows if e['event'] == 'content_filter_initial_submitted')
    config = next(e for e in rows if e['event'] == 'content_filter_initial_configuration')['filter_receipt']
    stamp = submitted['monotonic_ns']
    config['creation_returned_monotonic_ns'] = stamp - 100
    submitted['filter_receipt']['creation_returned_monotonic_ns'] = stamp - 100
    config['api_results']['get_content_filter'].update(started_monotonic_ns=stamp + 100, completed_monotonic_ns=stamp + 200)
    config['api_results']['is_cft_enabled'].update(started_monotonic_ns=stamp + 300, completed_monotonic_ns=stamp + 400)
    config['monotonic_ns'] = stamp + 500
    token = protocol[-2]
    token.update(initial_content_filter=deepcopy(config), created_monotonic_ns=stamp - 100,
                 create_begin_monotonic_ns=protocol[1]['monotonic_ns'])
    for send in (e for e in rows if e['event'] == 'send'):
        receipt = send['publish_occurrence_receipt']
        scope = [e for e in rows if e.get('call_id') == receipt['call_id']]
        receipt.update(call_begin_monotonic_ns=scope[0]['monotonic_ns'],
                       trace_monotonic_ns=scope[1]['monotonic_ns'],
                       call_return_monotonic_ns=scope[2]['monotonic_ns'])
    ref = lambda e: dict(path='/authored/raw', sha256='a' * 64, event_index=e['event_index'])
    sends = [e for e in rows if e['event'] == 'send']
    deliveries = [native_delivery_identity(e, sends, rows, ref=ref)
                  for e in rows if e['event'] == 'receive' and e['logical_id'] == 'r0']
    assert all(deliveries)
    program = {'endpoints': [{'logical_id': 'r0', 'filter': {'expression': expression,
                            'parameters': config['requested']['parameters']}}], 'actions': [action]}
    decisions = {'observations': [], 'unknowns': [{'reason': 'unrelated_native_record'}] if unknown else []}
    value = content_filter_observations(program, rows, decisions=decisions,
                                       deliveries=deliveries, ref=ref, fixture=False)
    return value, rows, program, ref, deliveries


def test_unrelated_native_unknown_preserves_independent_disabled_deliveries():
    value, _, _, _, _ = authored_sample_contract(expression='', parameters=[], unknown=True)
    assert [s['status'] for s in value['sample_observations']] == ['delivered_while_disabled'] * 3
    assert all(s['sample_effect_observed'] is True and s['filter_effect_observed'] is None
               for s in value['sample_observations'])
    assert value['native_decision_unknowns'] and not value['evidence_conflicts']


def test_authenticated_unexpected_delivery_is_an_explicit_CFT_residual():
    value, _, _, _, _ = authored_sample_contract(parameters=["'%ABSENTTOKEN%'"], unknown=True)
    assert len(value['sample_observations']) == 3
    assert all(s.get('violation') == 'topology.cft_predicate_mismatch'
               and s['expected_decision'] is False for s in value['sample_observations'])
    assert value['review_required'] is True


def test_CFT_replay_signature_recomputes_the_original_sample_observations():
    from copy import deepcopy
    from r2f.campaign.topology_cft_observation import replay_cft_observations
    value, rows, program, ref, _ = authored_sample_contract(parameters=["'%ABSENTTOKEN%'"])
    signature = replay_cft_observations(program, rows, value, peer_reference=None, ref=ref)
    assert [s['sequence'] for s in signature] == [0, 2, 4]
    assert all(s['violation'] == 'topology.cft_predicate_mismatch' for s in signature)
    changed = deepcopy(value)
    changed['sample_observations'][0]['expected_decision'] = True
    with pytest.raises(ValueError, match='raw recomputation'):
        replay_cft_observations(program, rows, changed, peer_reference=None, ref=ref)


from test_topology_candidate_identity import row


def test_common_signature_retains_CFT_failure_and_rejects_summary_tampering(row):
    from copy import deepcopy
    from r2f.core.replay import conformance_signature
    from test_topology_native_conformance_signature import authored
    from test_topology_conformance_signature import rewrite
    from r2f.campaign.topology_cft_observation import content_filter_observations
    from r2f.campaign.topology_sample_attribution import native_delivery_identity
    _, rows, program, _, _ = authored_sample_contract(parameters=["'%ABSENTTOKEN%'"])
    authored(row, rows)
    row['final_input']['topology_program']['actions'] = program['actions']
    reader = next(e for e in row['final_input']['topology_program']['endpoints'] if e['logical_id'] == 'r0')
    reader['filter'] = program['endpoints'][0]['filter']
    def ref(event):
        return dict(path=row['raw_path'], sha256=row['raw_sha256'], event_index=event['event_index'])
    sends = [e for e in rows if e['event'] == 'send']
    deliveries = [native_delivery_identity(e, sends, rows, ref=ref) for e in rows if e['event'] == 'receive']
    row['typed_facts']['content_filter_observations'] = content_filter_observations(
        row['final_input']['topology_program'], rows, decisions={'observations': [], 'unknowns': []},
        deliveries=[d for d in deliveries if d is not None], ref=ref, fixture=False)
    row['oracle_verdict'] = row['oracle']['verdict'] = 'fail'
    row['oracle']['forbidden_residuals'] = ['topology.cft_predicate_mismatch']
    row['oracle']['cft_sample_checks'] = deepcopy(row['typed_facts']['content_filter_observations']['sample_observations'])
    rewrite(row, rows)
    signature = conformance_signature(row)
    assert signature is not None
    assert len(signature['typed_anomaly_signature']) == 3
    assert {s['residual'] for s in signature['typed_anomaly_signature']} == {'topology.cft_predicate_mismatch'}
    damaged = deepcopy(row)
    damaged['typed_facts']['content_filter_observations']['sample_observations'][0]['violation'] = None
    assert conformance_signature(damaged) is None


def test_native_reject_and_same_occurrence_delivery_remain_review_conflict():
    from r2f.campaign.topology_cft_observation import content_filter_observations
    from r2f.campaign.anomalies import _kind
    value, rows, program, ref, deliveries = authored_sample_contract(parameters=["'%ABSENTTOKEN%'"])
    sample = value['sample_observations'][0]
    decision = {key: sample[key] for key in ('action_id', 'reader', 'reader_generation', 'reader_gid', 'send_ref')}
    decision['decision'] = False
    observed = content_filter_observations(program, rows,
        decisions={'observations': [decision], 'unknowns': [{'reason': 'unrelated_native_record'}]},
        deliveries=deliveries, ref=ref, fixture=False)
    conflict = observed['sample_observations'][0]
    assert conflict['status'] == 'evidence_conflict'
    assert conflict['violation'] is None
    assert conflict['sample_effect_observed'] is None
    assert conflict['delivery_witness'] and conflict['native_decision']
    assert observed['review_required'] and observed['evidence_conflicts']
    assert _kind({'execution_stage': 'judged', 'oracle_verdict': 'pass',
                  'typed_facts': {'content_filter_observations': observed}}, None) == 'observation_insufficient'
