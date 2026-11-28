"""Observer-off controls use the same vendor and input, never native evidence."""
from copy import deepcopy
import json
from pathlib import Path

import pytest

from test_fastdds_peer_probe import build_manifest
from test_fastdds_peer_runner import native_profile, config_from_result
from test_topology_cycle import run_cycle
from test_topology_peer_unavailable import snapshot_worker


def test_planner_persists_observer_control_without_changing_input(tmp_path, build_manifest):
    path, _ = build_manifest
    observed = native_profile(path)
    control = dict(deepcopy(observed), observe_native_peers=False)
    on, _ = config_from_result(run_cycle(tmp_path / 'on', observed))
    off, row = config_from_result(run_cycle(tmp_path / 'off', control))
    assert off['effective_input_sha256'] == on['effective_input_sha256']
    assert off['final_input'] == on['final_input']
    assert off['fastdds_peer_instrumentation'] == on['fastdds_peer_instrumentation']
    assert off['observe_native_peers'] is row['observe_native_peers'] is False
    capsule = json.loads(Path(row['replay_path']).read_text())
    assert 'observe_native_peers' not in capsule['input']['effective_input']['final_input']
    assert row['candidate_eligible'] is False


@pytest.mark.parametrize('value', [None, 0, 1, 'false', [], {}])
def test_invalid_sampling_controls_are_rejected(tmp_path, build_manifest, value):
    from r2f.campaign.exploratory_input import validate_profile_fields
    path, _ = build_manifest
    doc = dict(native_profile(path), observe_native_peers=value)
    with pytest.raises(ValueError, match='native_peer_sampling_control'):
        validate_profile_fields(doc)


def test_sampling_control_requires_explicit_instrumentation():
    from r2f.instrumentation.fastdds_peer.activation import reference_from_config
    with pytest.raises(ValueError, match='native_peer_sampling_control'):
        reference_from_config({'observe_native_peers': False})


@pytest.mark.parametrize('kind', ['writer', 'reader'])
def test_worker_control_keeps_public_counts_and_never_claims_native_capture(snapshot_worker, kind):
    rows = snapshot_worker(kind, observer_case='constructor_error', observe_native_peers=False)
    selected, = [row for row in rows if row['event'] == 'native_peer_provider_selected']
    match, = [row for row in rows if row['event'] == 'match_snapshot']
    assert selected['observe_native_peers'] is False
    assert match['observe_native_peers'] is False
    assert match['public_count_before'] == match['public_count_after'] == 1
    assert match['peer_observation_limitation'] == 'native_peer_sampling_disabled_control'
    assert 'native_peer_observation' not in match
    assert 'matched_peer_gids' not in match


def test_liveliness_control_keeps_counts_without_instantiating_native_provider():
    from types import SimpleNamespace
    from r2f.runtime.topology_worker import _LivelinessPeerSampler
    reader = SimpleNamespace(get_publisher_count=lambda: 1)
    capture = _LivelinessPeerSampler(reader, 'rmw_fastrtps_cpp', 196, 'reader',
                                    instrumentation={'unloadable_fixture': True}, enabled=False)()
    assert capture['public_count_before'] == capture['public_count_after'] == 1
    assert capture['status'] == 'unknown'
    assert capture['reason'] == 'native_peer_sampling_disabled_control'
    assert capture['peer_gids_complete'] is False


@pytest.mark.parametrize('damage', ['selection_flag', 'created_flag', 'native_match', 'native_liveliness'])
def test_control_raw_cannot_contradict_sampling_selection(build_manifest, damage):
    from test_fastdds_peer_activation import config, reference
    from r2f.instrumentation.fastdds_peer.receipt import validate_execution_mode
    path, manifest = build_manifest
    cfg = dict(config(path), observe_native_peers=False)
    created = dict(event='endpoint_created', event_index=1, logical_id='w0', generation=1,
        endpoint_generation='g1', pid=100, pgid=100, process_start_ticks=10,
        run_id='offline-control', domain_id=196, observation_mode='instrumentation',
        observe_native_peers=False)
    selected = dict(created, event='native_peer_provider_selected', event_index=0,
        manifest_reference=reference(path), vendor_binary=manifest['binaries']['vendor'],
        provider_binary=manifest['binaries']['provider'], provider_abi=manifest['patch']['abi'],
        provider_source_sha256=manifest['provider_source_sha256'])
    rows = [selected, created]
    assert validate_execution_mode(rows, cfg)
    if damage == 'selection_flag':
        selected.pop('observe_native_peers')
    elif damage == 'created_flag':
        created['observe_native_peers'] = True
    else:
        event = dict(created, event='match_snapshot' if damage == 'native_match'
                     else 'topology_liveliness_peer_snapshot', event_index=2)
        event['native_peer_observation'] = {
            'schema': 'r2f.fastdds-native-matched-peer-observation/v1', 'complete': True}
        rows.append(event)
    with pytest.raises(ValueError, match='native_peer_sampling_control'):
        validate_execution_mode(rows, cfg)
