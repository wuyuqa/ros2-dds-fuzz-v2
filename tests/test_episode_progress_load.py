"""Read-only checkpoint-prefix recovery; authored offline records, not DDS restore."""
from copy import deepcopy
import json

import pytest

from r2f.campaign.episode import EpisodeStore, EpisodeStoreError, run_episode
from r2f.core.evidence import canonical_hash
from test_episode import Clock, action, episode, ok_perform


def progress(tmp_path, *, checkpoint=True):
    ep = episode(actions=[action(0), action(1), action(2)])
    output = run_episode(ep, perform=lambda a, s: ok_perform(), clock=Clock())
    store = EpisodeStore(tmp_path / 'store')
    store.commit_progress(ep, output['receipts'][:2], checkpoint=checkpoint)
    path = store.root / 'checkpoints' / ep.episode_id / '0001.json'
    return store, ep, path


def test_load_partial_progress_without_terminal_is_read_only_and_copy_isolated(tmp_path):
    store, ep, path = progress(tmp_path)
    original = path.read_bytes()
    inventory = sorted(str(p.relative_to(store.root)) for p in store.root.rglob('*'))
    assert not (store.root / 'episodes').exists()
    record = store.load_progress(ep, 1)
    assert record == json.loads(original)
    assert record['restoration_supported'] is False
    assert all(r['oracle_verdict'] == 'unknown' for r in record['receipts'])
    record['receipts'][0]['parameters']['mutated_return'] = True
    assert store.load_progress(ep, 1) == json.loads(original)
    assert path.read_bytes() == original
    assert sorted(str(p.relative_to(store.root)) for p in store.root.rglob('*')) == inventory


def rewrite(path, edit, *, rehash=True):
    record = json.loads(path.read_text())
    edit(record)
    if rehash:
        record['record_sha256'] = canonical_hash({k: v for k, v in record.items() if k != 'record_sha256'})
    path.write_text(json.dumps(record))
    return path.read_bytes()


@pytest.mark.parametrize('sequence', [True, False, -1, 3, 1.0, '1', None])
def test_sequence_is_nonbool_ordinal_in_range_without_creating_paths(tmp_path, sequence):
    ep = episode(actions=[action(0), action(1), action(2)])
    root = tmp_path / 'never-created'
    with pytest.raises(EpisodeStoreError, match='progress_sequence_out_of_range'):
        EpisodeStore(root).load_progress(ep, sequence)
    assert not root.exists()


def test_missing_progress_and_wrong_episode_type_are_named_and_read_only(tmp_path):
    root = tmp_path / 'missing'
    store = EpisodeStore(root)
    with pytest.raises(EpisodeStoreError, match='progress_episode_required'):
        store.load_progress({}, 0)
    with pytest.raises(EpisodeStoreError, match='progress_invalid:FileNotFoundError'):
        store.load_progress(episode(), 0)
    assert not root.exists()


def test_checkpoint_false_loads_without_becoming_resume_authorization(tmp_path):
    store, ep, path = progress(tmp_path, checkpoint=False)
    loaded = store.load_progress(ep, 1)
    assert loaded['checkpoint'] is False and loaded['restoration_supported'] is False
    assert loaded == json.loads(path.read_text())


@pytest.mark.parametrize('field,value,reason', [
    ('schema', 'wrong', 'schema_mismatch'),
    ('episode_id', 'different-episode', 'episode_id_mismatch'),
    ('input_sha256', 'a' * 64, 'input_identity_mismatch'),
    ('state_schema', 'wrong', 'state_schema_mismatch'),
    ('action_count', 1, 'action_count_mismatch'),
    ('action_count', 2.0, 'action_count_mismatch'),
    ('action_count', True, 'action_count_mismatch'),
    ('checkpoint', 1, 'checkpoint_not_bool'),
    ('restoration_supported', True, 'restoration_not_false'),
    ('restoration_supported', 0, 'restoration_not_false'),
    ('receipts', [], 'receipt_count_mismatch'),
    ('receipts', {}, 'receipt_count_mismatch'),
])
def test_rehashed_record_still_binds_all_progress_metadata(tmp_path, field, value, reason):
    store, ep, path = progress(tmp_path)
    edited = rewrite(path, lambda r: r.__setitem__(field, value))
    with pytest.raises(EpisodeStoreError, match=reason):
        store.load_progress(ep, 1)
    assert path.read_bytes() == edited


@pytest.mark.parametrize('edit', [lambda r: r.pop('checkpoint'),
                                 lambda r: r.__setitem__('unknown_field', True)])
def test_top_level_missing_or_unknown_fields_are_rejected(tmp_path, edit):
    store, ep, path = progress(tmp_path)
    rewrite(path, edit)
    with pytest.raises(EpisodeStoreError, match='record_fields_invalid'):
        store.load_progress(ep, 1)


def test_hash_mismatch_is_not_fixed_on_read(tmp_path):
    store, ep, path = progress(tmp_path)
    edited = rewrite(path, lambda r: r.__setitem__('checkpoint', False), rehash=False)
    with pytest.raises(EpisodeStoreError, match='record_hash_mismatch'):
        store.load_progress(ep, 1)
    assert path.read_bytes() == edited


@pytest.mark.parametrize('field,value,reason', [
    ('schema', 'wrong', 'schema_mismatch'),
    ('sequence', 0, 'action_binding_mismatch'),
    ('sequence', True, 'action_binding_mismatch'),
    ('action_id', 'wrong', 'action_binding_mismatch'),
    ('action', 'different', 'action_binding_mismatch'),
    ('input_sha256', 'b' * 64, 'action_binding_mismatch'),
    ('parameters', {'forged': True}, 'parameters_mismatch'),
    ('expected_effect', ['forged'], 'expected_effect_mismatch'),
    ('before_state_id', 'c' * 64, 'before_state_id_mismatch'),
    ('after_state_id', 'd' * 64, 'after_state_id_mismatch'),
    ('execution_stage', 'fake-success', 'unsupported execution_stage'),
    ('oracle_verdict', 'maybe', 'oracle verdict'),
    ('oracle_verdict', [], 'verdict_invalid'),
    ('tool_error', 0, 'tool_error_not_bool'),
])
def test_rehashed_receipts_are_independently_validated(tmp_path, field, value, reason):
    store, ep, path = progress(tmp_path)
    rewrite(path, lambda r: r['receipts'][1].__setitem__(field, value))
    with pytest.raises(EpisodeStoreError, match=reason):
        store.load_progress(ep, 1)


def test_receipt_order_missing_and_unknown_fields_fail_closed(tmp_path):
    store, ep, path = progress(tmp_path)
    original = path.read_bytes()
    for edit in (lambda r: r['receipts'].reverse(),
                 lambda r: r['receipts'][0].pop('action'),
                 lambda r: r['receipts'][0].__setitem__('unknown_field', {})):
        path.write_bytes(original)
        rewrite(path, edit)
        with pytest.raises(EpisodeStoreError):
            store.load_progress(ep, 1)


@pytest.mark.parametrize('fault', ['first_before', 'second_before', 'first_after', 'last_after'])
def test_rehashed_state_tamper_cannot_break_initial_or_between_action_chain(tmp_path, fault):
    from r2f.core.state_signature import state_id
    store, ep, path = progress(tmp_path)
    def edit(record):
        receipts = record['receipts']
        row = receipts[0 if fault in {'first_before', 'first_after'} else 1]
        side = 'before' if 'before' in fault else 'after'
        altered = deepcopy(row[side + '_state'])
        altered['network_phase'] = 'drop_active'
        row[side + '_state'] = altered
        if fault != 'last_after':
            # Rehash the state id too: initial/prefix links still reject it.
            row[side + '_state_id'] = state_id(altered)
    rewrite(path, edit)
    with pytest.raises(EpisodeStoreError, match='state_chain_mismatch|after_state_id_mismatch'):
        store.load_progress(ep, 1)


def test_changed_input_episode_and_mutated_cached_action_identity_are_rejected(tmp_path):
    store, ep, path = progress(tmp_path)
    from dataclasses import replace
    changed = replace(ep, limits=replace(ep.limits, max_seconds=30))
    with pytest.raises(EpisodeStoreError, match='input_identity_mismatch'):
        store.load_progress(changed, 1)
    ep.actions[0].input_entry['final_config']['new_input'] = True
    with pytest.raises(EpisodeStoreError, match='progress_invalid'):
        store.load_progress(ep, 1)


def test_optional_observation_and_unknown_verdict_roundtrip_unmodified(tmp_path):
    ep = episode(actions=[action(0), action(1)])
    output = run_episode(ep, perform=lambda a, s: ok_perform(observation={'raw_ref': {'event_index': a.sequence}}), clock=Clock())
    store = EpisodeStore(tmp_path)
    store.commit_progress(ep, output['receipts'], checkpoint=True)
    record = store.load_progress(ep, 1)
    assert record['receipts'] == output['receipts']
    assert all(r['oracle_verdict'] == 'unknown' for r in record['receipts'])


def test_tool_failure_null_after_is_loadable_but_not_restored_or_passed(tmp_path):
    ep = episode()
    def fail(a, s):
        raise RuntimeError('fixture failure')
    output = run_episode(ep, perform=fail, clock=Clock())
    store = EpisodeStore(tmp_path)
    store.commit_progress(ep, output['receipts'])
    record = store.load_progress(ep, 0)
    receipt = record['receipts'][0]
    assert receipt['after_state'] is None and receipt['after_state_id'] is None
    assert receipt['execution_stage'] == 'aborted' and receipt['tool_error'] is True
    assert receipt['oracle_verdict'] is None
    assert record['restoration_supported'] is False


def test_null_after_requires_null_state_id(tmp_path):
    store, ep, path = progress(tmp_path)
    rewrite(path, lambda r: r['receipts'][1].__setitem__('after_state', None))
    with pytest.raises(EpisodeStoreError, match='null_after_state_id_mismatch'):
        store.load_progress(ep, 1)


def test_pass_needs_existing_rule_but_is_never_inferred_by_loader(tmp_path):
    store, ep, path = progress(tmp_path)
    rewrite(path, lambda r: r['receipts'][1].update(oracle_verdict='pass', rule_id=''))
    with pytest.raises(EpisodeStoreError, match='rule_id_missing'):
        store.load_progress(ep, 1)


def test_topology_state_schema_and_actual_transition_load_via_existing_validator(tmp_path):
    from dataclasses import replace
    from test_episode_state_schema import topology_state, topology_episode
    ep = replace(topology_episode(), actions=[action(0), action(1)])
    output = run_episode(ep, perform=lambda a, s: dict(status='executed', execution_stage='judged',
        after_state=topology_state(2), oracle_verdict='unknown'), clock=Clock())
    store = EpisodeStore(tmp_path)
    store.commit_progress(ep, output['receipts'], checkpoint=True)
    record = store.load_progress(ep, 1)
    assert record['state_schema'] == ep.state_schema
    assert record['receipts'][0]['before_state_id'] == ep.initial_state_id
    assert record['receipts'][1]['before_state_id'] == record['receipts'][0]['after_state_id']
    assert record['receipts'][1]['before_state_id'] != ep.initial_state_id


@pytest.mark.parametrize('text', ['{', '{"schema":1,"schema":2}', '{"value":NaN}', '{"value":1e400}'])
def test_invalid_duplicate_or_nonfinite_json_is_named_and_not_rewritten(tmp_path, text):
    store, ep, path = progress(tmp_path)
    path.write_text(text)
    before = path.read_bytes()
    with pytest.raises(EpisodeStoreError, match='progress_'):
        store.load_progress(ep, 1)
    assert path.read_bytes() == before
