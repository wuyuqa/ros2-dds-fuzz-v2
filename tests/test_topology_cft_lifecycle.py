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


def initial_rows(*, real_envelope=False):
    """Public creation API double; explicitly NON-DDS and not persisted evidence."""
    from copy import deepcopy
    import time
    from test_topology_content_filter import FakeNode, FakeOptions
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
    _, receipt = _create_reader_with_initial_filter(
        FakeNode(), object, lineage['topic'], lambda _: None, object,
        {'expression': 'data = %0', 'parameters': ['keep']},
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


@pytest.mark.parametrize('damage', ['getter', 'state', 'bool_alias', 'identity', 'clock', 'raw_ref'])
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
