"""Authored raw fixtures for native membership; not a native/DDS execution."""
from copy import deepcopy
import hashlib
from pathlib import Path
from types import SimpleNamespace

import pytest

from r2f.instrumentation.fastdds_peer import receipt as r
from r2f.runtime.topology_pair import _delivery_window_evidence
from r2f.runtime.topology_worker import _matched_peer_fields
from test_topology_publish_occurrence import native_window


@pytest.fixture(autouse=True)
def authored_manifest_loader(monkeypatch):
    # This file authors raw contracts only. Native/noDDS manifest validation is
    # separately tested against real build artifacts by main and runner tests.
    vendor = dict(path='/offline/lib/libfastrtps.so.2.14.7', sha256='b' * 64)
    provider = dict(path='/offline/lib/libr2f_fastdds_peer.so', sha256='c' * 64)
    manifest = dict(binaries=dict(vendor=vendor, provider=provider),
        provider_source_sha256=hashlib.sha256(Path(r.__file__).with_name('provider.cpp').read_bytes()).hexdigest(),
        patch={'abi':dict(schema_version=1, result_size=64, guid_size=16,
                          layout_changed=False, virtual_slots_added=False)})
    monkeypatch.setattr(r, 'manifest_for_reference', lambda reference: deepcopy(manifest))


def instrumented_window():
    rows, opened, close = native_window()
    ref = dict(observation_mode='instrumentation', manifest_path='/offline/build-manifest.json',
               manifest_sha256='a' * 64)
    providers = []
    for token in rows[:2]:
        token['observation_mode'] = 'instrumentation'
        vendor = dict(path='/offline/lib/libfastrtps.so.2.14.7', sha256='b' * 64)
        provider = dict(path='/offline/lib/libr2f_fastdds_peer.so', sha256='c' * 64)
        mapped = [dict(role=role, **binary, inode=123 + i, device='08:02')
                  for i, (role, binary) in enumerate((('vendor', vendor), ('provider', provider)))]
        selected = dict(token, event='native_peer_provider_selected', manifest_reference=deepcopy(ref),
            observed_domain_id=token['domain_id'], monotonic_ns=10190,
            vendor_identity=dict(mapped[0], identity_verified=True, membership_verified=False,
                                 observation_mode='instrumentation', manifest_reference=deepcopy(ref)),
            provider_binary=provider, vendor_binary=vendor,
            provider_source_sha256=hashlib.sha256(Path(r.__file__).with_name('provider.cpp').read_bytes()).hexdigest(),
            provider_abi=dict(schema_version=1, result_size=64, guid_size=16,
                              layout_changed=False, virtual_slots_added=False))
        providers.append(selected)
        for match in rows:
            if match['event'] != 'match_snapshot' or match['logical_id'] != token['logical_id']:
                continue
            count = match['matched_count']
            match.update(observation_mode='instrumentation', public_count_before=count,
                         peer_proof_source='fastdds_native_stateful_peer_snapshot')
            match['native_peer_observation'] = dict(schema=r.OBSERVATION_SCHEMA,
                observation_mode='instrumentation', complete=True, domain_id=token['domain_id'],
                local_gid=token['endpoint_gid'], role_kind=token['role'],
                matched_peer_gids=deepcopy(match['matched_peer_gids']),
                membership_basis=r.MEMBERSHIP_BASIS, snapshot_ordinal_basis=r.ORDINAL_BASIS,
                manifest_reference=deepcopy(ref), mapped_libraries=mapped,
                started_monotonic_ns=match['monotonic_ns'], ended_monotonic_ns=match['monotonic_ns'],
                native_result=dict(schema_version=1, struct_size=64, status=0,
                    endpoint_kind=1 if token['role']=='writer' else 2, local_guid=token['endpoint_gid'],
                    required_count=count, copied=count, snapshot_ordinal=11, complete=1, reserved=0))
    # Preserve reference links as objects when inserting new raw declarations.
    refs = {key: [rows[i] for i in indices] for key, indices in opened['snapshot_refs'].items()}
    old_open = opened['event_index']
    rows[3:3] = providers
    for i, row in enumerate(rows): row['event_index'] = i
    opened['snapshot_start_ref'] = 3 + len(providers)
    opened['snapshot_refs'] = {key: [row['event_index'] for row in chain] for key,chain in refs.items()}
    for row in rows:
        if row.get('open_ref') == old_open: row['open_ref'] = opened['event_index']
    return rows, opened, close


def proof(rows, opened, close):
    return _delivery_window_evidence(opened, rows, before_index=close['event_index'])


def test_complete_bidirectional_native_set_closes_same_original_window():
    rows, opened, close = instrumented_window()
    result = proof(rows, opened, close)
    assert result['window_complete'], result
    assert result['received_sequences'] == [0,2,4]


@pytest.mark.parametrize('damage', ['native_missing', 'short_copy', 'wrong_guid', 'wrong_role',
    'wrong_manifest', 'wrong_source', 'wrong_binary', 'wrong_inode', 'wrong_status',
    'wrong_domain', 'ordinal_as_epoch', 'no_selection', 'duplicate_selection',
    'selection_other_process', 'selection_after_capture', 'capture_before_request',
    'pretend_public', 'remove_all_match_labels'])
def test_bad_native_receipt_never_falls_back_or_closes(damage):
    rows, opened, close = instrumented_window()
    match = next(row for row in rows if row['event']=='match_snapshot')
    obs = match['native_peer_observation']
    selected = next(row for row in rows if row['event']=='native_peer_provider_selected')
    if damage == 'native_missing': match.pop('native_peer_observation')
    elif damage == 'short_copy': obs['native_result']['copied'] -= 1
    elif damage == 'wrong_guid': obs['native_result']['local_guid'] = 'f' * 32
    elif damage == 'wrong_role': obs['native_result']['endpoint_kind'] = 2
    elif damage == 'wrong_manifest': obs['manifest_reference']['manifest_sha256'] = 'f' * 64
    elif damage == 'wrong_source': selected['provider_source_sha256'] = 'f' * 64
    elif damage == 'wrong_binary': obs['mapped_libraries'][1]['sha256'] = 'f' * 64
    elif damage == 'wrong_inode': obs['mapped_libraries'][0]['inode'] += 1
    elif damage == 'wrong_status': obs['native_result']['status'] = 4
    elif damage == 'wrong_domain': obs['domain_id'] += 1
    elif damage == 'ordinal_as_epoch': obs['snapshot_ordinal_basis'] = 'match_epoch'
    elif damage == 'no_selection': selected['event'] = 'irrelevant'
    elif damage == 'duplicate_selection':
        other = next(row for row in rows if row['event']=='native_peer_provider_selected' and row is not selected)
        for key in ('logical_id','generation','endpoint_generation','pid','pgid','process_start_ticks','run_id','domain_id'):
            other[key] = selected[key]
    elif damage == 'selection_other_process': selected['process_start_ticks'] += 1
    elif damage == 'selection_after_capture': selected['monotonic_ns'] = match['monotonic_ns']+1
    elif damage == 'capture_before_request': obs['started_monotonic_ns'] = 1
    elif damage == 'pretend_public': match['peer_proof_source'] = 'dds_matched_endpoint_api'
    else:
        for key in ('peer_proof_source','peer_gids_complete','native_peer_observation','observation_mode'):
            match.pop(key, None)
    assert not proof(rows, opened, close)['window_complete']


def test_worker_does_not_label_bad_native_schema_as_public_api():
    observer = SimpleNamespace(SCHEMA=r.OBSERVATION_SCHEMA)
    obs = dict(schema='wrong-schema', complete=True, local_gid='1'*32, domain_id=191,
               role_kind='writer', matched_peer_gids=['2'*32])
    fields = _matched_peer_fields(observer, 191, '1'*32, 'writer', 1,1,obs)
    assert fields['peer_gids_complete'] is False
    assert 'peer_proof_source' not in fields


def test_reused_native_snapshot_cannot_be_relabelled_as_fresh_raw_command():
    rows, opened, close = instrumented_window()
    match = next(row for row in rows if row['event']=='match_snapshot')
    other = next(row for row in rows if row['event']=='native_peer_provider_selected'
                 and row['logical_id'] != match['logical_id'])
    selected = next(row for row in rows if row['event']=='native_peer_provider_selected'
                    and row['logical_id'] == match['logical_id'])
    # Authored independent older receipt before the new request, same provider
    # process, non-increasing native observation serial. No persisted raw edits.
    other.clear()
    other.update(deepcopy(match), event_index=selected['event_index']+1, monotonic_ns=10191)
    assert not proof(rows, opened, close)['window_complete']


def test_native_raw_cannot_be_relabelled_as_blackbox_execution():
    rows, _, _ = instrumented_window()
    with pytest.raises(ValueError, match='actual_mode_differs_from_blackbox'):
        r.validate_execution_mode(rows, {})


@pytest.mark.parametrize('damage', ['hash', 'path'])
def test_jointly_forged_raw_provider_identity_rejected_by_manifest(damage):
    rows, opened, close = instrumented_window()
    selected = next(row for row in rows if row['event']=='native_peer_provider_selected')
    match = next(row for row in rows if row['event']=='match_snapshot')
    mapped = match['native_peer_observation']['mapped_libraries'][1]
    if damage == 'hash':
        selected['provider_binary']['sha256'] = mapped['sha256'] = 'd'*64
    else:
        selected['provider_binary']['path'] = mapped['path'] = '/forged/libr2f_fastdds_peer.so'
    assert not proof(rows, opened, close)['window_complete']
