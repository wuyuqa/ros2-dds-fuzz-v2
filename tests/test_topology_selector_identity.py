"""Pure-input regression: narrow topology selector projection, no execution."""
from copy import deepcopy
from pathlib import Path
import json

import pytest

from r2f.campaign.corpus import (
    CONTENT_IDENTITY_SCHEMA, TOPOLOGY_CONTENT_IDENTITY_SCHEMA,
    Corpus, content_identity, content_sha256, corpus_from_profile,
)
from r2f.campaign.search_frontier import InputSearchFrontier, derived_record
from r2f.core.evidence import canonical_hash, effective_input_hash, INPUT_SCHEMA_V2
from r2f.core.mutation_plan import RUNTIME_WINDOW_SCHEMA
from r2f.modules.s1.topology_input import make_topology_plan

PROFILE = Path(__file__).resolve().parents[1] / 'config/s1_fuzz/profiles/model-bfs-topology-runtime-smoke.json'


@pytest.fixture(scope='module')
def parent():
    return next(iter(corpus_from_profile(PROFILE, cycle_id='topology-authored').entries.values()))


def generated(parent, label, operator='queue.depth', extra=None):
    request = {'operator_id': operator, 'slot_id': 'S1.topology.qos.depth',
               'context': {'boundary_class': label, **(extra or {})}}
    plan = make_topology_plan(parent['final_config']['topology_program'], [request], rng_seed=41)
    return derived_record(parent, plan)


def detached(record):
    record = deepcopy(record)
    record.pop('mutation_plan')
    record.pop('content_sha256')
    return record


@pytest.mark.parametrize('operator', ['queue.depth', 'param.boundary'])
def test_same_submission_same_content_full_plan_and_effective_binding_distinct(parent, operator):
    a, b = (generated(parent, label, operator) for label in ('min', 'one'))
    saved = deepcopy((a, b))
    assert a['final_config'] == b['final_config']
    assert a['runtime_windows'] == b['runtime_windows']
    assert a['content_sha256'] == b['content_sha256']
    assert canonical_hash(a['mutation_plan']) != canonical_hash(b['mutation_plan'])
    from test_fuzz_identity import legacy_input
    def effective(record):
        data = legacy_input()
        data.update({key: record[key] for key in ('target_id', 'target_revision', 'rmw', 'contract_hash', 'mutation_plan', 'operations', 'runtime_windows')})
        data.update(input_identity_schema=INPUT_SCHEMA_V2, final_input=record['final_config'])
        return effective_input_hash(data)
    assert effective(a) != effective(b)
    assert (a, b) == saved


@pytest.mark.parametrize('operator', ['queue.depth', 'param.boundary'])
def test_different_concrete_depth_not_merged(parent, operator):
    a, b = (generated(parent, label, operator) for label in ('min', 'max'))
    assert a['final_config'] != b['final_config']
    assert a['content_sha256'] != b['content_sha256']


def test_domain_is_topology_only_root_and_derived_old_corpus_rejected(parent):
    assert CONTENT_IDENTITY_SCHEMA == 'r2f.corpus-content/v2'
    assert TOPOLOGY_CONTENT_IDENTITY_SCHEMA == 'r2f.corpus-content/topology-v3'
    root = {k: deepcopy(parent[k]) for k in ('target_id', 'target_revision', 'rmw', 'contract_hash', 'final_config')}
    assert content_identity(root) == canonical_hash({'schema': TOPOLOGY_CONTENT_IDENTITY_SCHEMA, 'identity': root})
    old = canonical_hash({'schema': CONTENT_IDENTITY_SCHEMA, 'identity': root})
    assert old != content_identity(root)
    with pytest.raises(ValueError, match='content hash mismatch'):
        Corpus().add_seed(dict(root, content_sha256=old))
    c = Corpus(); c.add_seed(root)
    data = c.to_dict(); data['entries'][0]['content_sha256'] = old
    with pytest.raises(ValueError, match='content hash mismatch'):
        Corpus.from_dict(data)
    for record in (parent, generated(parent, 'min')):
        assert content_sha256(record) == record['content_sha256']


@pytest.mark.parametrize('field,value', [('target_revision', 1), ('rmw', 'rmw_fastrtps_cpp'), ('contract_hash', 'a'*64)])
def test_revision_rmw_contract_remain_distinct(parent, field, value):
    a = detached(generated(parent, 'min')); b = deepcopy(a); b[field] = value
    assert content_identity(a) != content_identity(b)


@pytest.mark.parametrize('key', ['unknown', 'history', 'seed', 'path', 'boundary_class'])
def test_actual_payload_keys_are_lossless(parent, key):
    a = detached(generated(parent, 'min')); b = deepcopy(a)
    a['final_config']['topology_program']['diagnostic_payload'] = {key: 'a'}
    b['final_config']['topology_program']['diagnostic_payload'] = {key: 'b'}
    assert content_identity(a) != content_identity(b)


@pytest.mark.parametrize('key', ['unknown', 'history', 'payloadkeys', 'parameters'])
def test_unknown_context_and_nested_parameters_stay_hashed(parent, key):
    a = generated(parent, 'min', extra={key: {'boundary_class': 'min'}})
    b = generated(parent, 'one', extra={key: {'boundary_class': 'one'}})
    assert a['final_config'] == b['final_config']
    assert a['content_sha256'] != b['content_sha256']


@pytest.mark.parametrize('change', ['runtime', 'operator', 'slot', 'mutator', 'surface', 'target', 'program_schema', 'extra_envelope', 'unresolved_slot'])
def test_projection_never_widens_to_unknown_or_runtime_operations(parent, change):
    a, b = (detached(generated(parent, label)) for label in ('min', 'one'))
    for record in (a, b):
        op = record['operations'][0]; p = op['parameters']
        if change == 'runtime': op['application_phase'] = 'runtime_window'
        elif change == 'operator': p['operator_id'] = p['request']['operator_id'] = 'unknown'
        elif change == 'slot': p['slot_id'] = p['request']['slot_id'] = 'S1.topology.other'
        elif change == 'unresolved_slot': p['slot_id'] = 'S1.topology.qos.depth'
        elif change == 'mutator': op['mutator_id'] = 'unknown'
        elif change == 'surface': op['surface_id'] = 'unknown'
        elif change == 'target': record['target_id'] = 'other'
        elif change == 'program_schema': record['final_config']['topology_program']['schema'] = 'unknown'
        else: p['extra'] = True
    assert content_identity(a) != content_identity(b)


def test_guard_and_window_values_and_order_stay_distinct(parent):
    a = detached(generated(parent, 'min')); b = deepcopy(a)
    b['operations'][0]['applicability_result']['expected_disposition'] = 'expect_guard_reject'
    assert content_identity(a) != content_identity(b)
    window = {'schema': RUNTIME_WINDOW_SCHEMA, 'window_id': 'w1', 'window_index': 0,
              'trigger_anchor': 'fresh_match', 'trigger_offset_sec': 0,
              'drop_duration_sec': .1, 'restore_interval_sec': .2}
    a['runtime_windows'] = [window]; b = deepcopy(a)
    b['runtime_windows'][0]['drop_duration_sec'] = .3
    assert content_identity(a) != content_identity(b)
    a['runtime_windows'].append(dict(window, window_id='w2', window_index=1, drop_duration_sec=.4))
    b = deepcopy(a)
    b['runtime_windows'][0]['drop_duration_sec'], b['runtime_windows'][1]['drop_duration_sec'] = .4, .1
    assert content_identity(a) != content_identity(b)


def test_multi_operator_same_seed_chain_and_operation_order(parent):
    request = lambda label: {'operator_id': 'queue.depth', 'slot_id': 'S1.topology.qos.depth', 'context': {'boundary_class': label}}
    relation = {'operator_id': 'relation.pair', 'slot_id': 'S1.topology.qos.reliability', 'context': {'pair': 'RR'}}
    a, b = [derived_record(parent, make_topology_plan(parent['final_config']['topology_program'], [request(label), relation], rng_seed=41)) for label in ('min', 'one')]
    assert len(a['operations']) == 2
    assert a['final_config'] == b['final_config']
    assert a['content_sha256'] == b['content_sha256']
    a = detached(a); b = deepcopy(a); b['operations'].reverse()
    assert content_identity(a) != content_identity(b)


def test_frontier_duplicate_content_not_new_child(parent):
    profile = json.loads(PROFILE.read_text())
    from r2f.campaign.topology_search import descriptor
    desc = descriptor(profile['topology_search'], profile['operator_whitelist'])
    f = InputSearchFrontier([parent], profile['search'], rng_seed=41,
        sampling_kinds=None, sampling_weight=None, topology_descriptor=desc)
    tasks = [t for t in f.tasks if t['derivation_depth'] == 1]
    a, b = (generated(parent, label) for label in ('min', 'one'))
    tasks[0]['status'] = 'selected'; f.planned(tasks[0]['task_id'], a)
    before = len(f.nodes)
    tasks[1]['status'] = 'selected'; f.planned(tasks[1]['task_id'], b)
    assert tasks[0]['generation_outcome'] == 'new_input'
    assert tasks[1]['generation_outcome'] == 'duplicate_content'
    assert len(f.nodes) == before


def test_generic_f1_and_legacy_hashes_are_byte_identical_to_fixed_local_base():
    # Captured from the unmodified corpus.py at HEAD 8b194c8a, not regenerated
    # goldens. This topology-only change must not reorder unrelated F1 anchors.
    from test_corpus_content_semantics import baseline, planned, attach_provider_layer
    from test_fuzz_identity import legacy_input
    assert content_identity(baseline()) == '4b99883ba3560c92f5957821118fb694f1519235de295811dff436f9d6b86b9c'
    assert content_identity(planned()) == '6cc3e3097b53a5f1f335a50f477860e5500cbc246cba2505dfac081671824eb4'
    assert content_identity(attach_provider_layer(planned())) == '0dde11d461c5affb0b74032e1930c2f76de9521be2e32ade9f9cbe8f05e12868'
    assert effective_input_hash(legacy_input()) == 'e626ce490eb4bb3afa94bcf1b0540cb388f2453414c113fb67abef9356b563f0'


def test_old_derived_content_domain_is_rejected_without_migration(parent):
    from r2f.campaign.corpus import INPUT_IDENTITY_KEYS, _content_operations
    record = generated(parent, 'min')
    identity = {key: deepcopy(record[key]) for key in INPUT_IDENTITY_KEYS if key in record}
    identity.update(final_config=deepcopy(record['final_config']),
                    operations=_content_operations(record['operations']),
                    runtime_windows=deepcopy(record['runtime_windows']))
    old = canonical_hash({'schema': CONTENT_IDENTITY_SCHEMA, 'identity': identity})
    assert old != record['content_sha256']
    saved = deepcopy(record)
    with pytest.raises(ValueError, match='content hash mismatch'):
        content_sha256(dict(record, content_sha256=old))
    assert record == saved


def test_resolved_endpoint_slot_identity_is_not_erased(parent):
    a = detached(generated(parent, 'min')); b = deepcopy(a)
    b['operations'][0]['parameters']['slot_id'] = 'S1.topology.qos.depth#0'
    assert a['operations'][0]['parameters']['slot_id'] != 'S1.topology.qos.depth#0'
    assert content_identity(a) != content_identity(b)
