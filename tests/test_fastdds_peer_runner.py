"""Unified planner carries explicit observation metadata, never execution proof."""
from copy import deepcopy
import json
from pathlib import Path
from unittest.mock import patch

import pytest

from test_fastdds_peer_probe import build_manifest
from test_fastdds_peer_activation import config
from test_topology_cycle import profile, run_cycle
from r2f.campaign.exploratory_input import validate_profile_fields
from r2f.runtime.topology_route import validate_topology_route
from r2f.runtime.topology_worker import RosTopologyBackend


def native_profile(path):
    doc = profile(1)
    doc['rmws'] = ['rmw_fastrtps_cpp']
    selected = config(path)
    doc.update({k: selected[k] for k in ('observation_mode', 'fastdds_peer_instrumentation')})
    return doc


def config_from_result(result):
    row, = result['rows']
    return json.loads(Path(row['runtime_config_path']).read_text()), row


def test_unified_planner_preserves_input_identity_and_persists_native_selection(tmp_path, build_manifest):
    path, _ = build_manifest
    native = native_profile(path)
    blackbox = deepcopy(native)
    blackbox.pop('observation_mode'); blackbox.pop('fastdds_peer_instrumentation')
    with patch('ctypes.CDLL', side_effect=AssertionError('NO NATIVE LOAD')):
        original, oldrow = config_from_result(run_cycle(tmp_path/'black', blackbox))
        selected, row = config_from_result(run_cycle(tmp_path/'native', native))
    assert selected['effective_input_sha256'] == original['effective_input_sha256']
    assert selected['final_input'] == original['final_input']
    assert selected['observation_mode'] == row['observation_mode'] == 'instrumentation'
    assert selected['fastdds_peer_instrumentation'] == native['fastdds_peer_instrumentation']
    assert row['fastdds_peer_instrumentation'] == native['fastdds_peer_instrumentation']
    assert 'fastdds_peer_instrumentation' not in original
    assert not row['actual_run'] and row['candidate_eligible'] is False
    validate_topology_route(selected)


@pytest.mark.parametrize('change', ['wrong_vendor', 'missing_mode', 'bad_pin', 'unsupported_generic_knob'])
def test_profile_never_silently_ignores_native_controls(build_manifest, change):
    path, _ = build_manifest
    doc = native_profile(path)
    if change == 'wrong_vendor': doc['rmws'] = ['rmw_cyclonedds_cpp']
    elif change == 'missing_mode': doc.pop('observation_mode')
    elif change == 'bad_pin': doc['fastdds_peer_instrumentation']['manifest_sha256'] = '0'*64
    else: doc['skip_native_verification'] = True
    with pytest.raises(ValueError):
        validate_profile_fields(doc)


def test_backend_keeps_manifest_and_override_inside_explicit_worker_environment(tmp_path, build_manifest):
    path, selected = build_manifest
    cfg = dict(config(path), live_execution_go=True, run_id='offline-activation', domain_id=191)
    backend = RosTopologyBackend(cfg, tmp_path/'runtime')
    backend.execution_preflight = {'ok': True, 'source': {}}
    backend.execution_environment = {'PATH': '/usr/bin', 'LD_LIBRARY_PATH': '/opt/ros/jazzy/lib'}
    with patch('r2f.runtime.ros2_pair.verify_pair_lease', return_value=['offline-lock']*3), \
         patch('ctypes.CDLL', side_effect=AssertionError('NO NATIVE LOAD')):
        receipt = backend.prepare({'environment': {}}, cfg)
    copied = backend.output_dir/'native-peer-build-manifest.json'
    assert copied.read_bytes() == path.read_bytes()
    assert receipt['native_peer_instrumentation']['manifest_path'] == str(copied)
    assert backend.environment['LD_LIBRARY_PATH'].startswith(str(Path(selected['install'])/'lib')+':')
    assert backend.execution_environment['LD_LIBRARY_PATH'] == '/opt/ros/jazzy/lib'
    assert not receipt['execution_evidence']


def test_report_binds_observation_selection_in_profile_row_and_config(build_manifest):
    from r2f.reports.run_report import _verify_topology_binding
    path, _ = build_manifest
    cfg = config(path)
    # This mismatch must be caught before the existing plan/binding gates.
    with pytest.raises(ValueError, match='topology_observation_selection_mismatch'):
        _verify_topology_binding(cfg, None, {}, {'profile': cfg})


def test_actual_native_selection_matches_config_not_only_same_worker_labels(build_manifest):
    from test_fastdds_peer_receipt import instrumented_window
    from r2f.instrumentation.fastdds_peer import receipt as evidence
    from test_fastdds_peer_activation import reference
    path, manifest = build_manifest
    rows, _, _ = instrumented_window()
    selected = [row for row in rows if row['event']=='native_peer_provider_selected']
    selected[0]['manifest_reference'] = reference(path)
    for row in selected:
        row['manifest_reference'] = reference(path)
        row['vendor_binary'] = manifest['binaries']['vendor']
        row['provider_binary'] = manifest['binaries']['provider']
        row['provider_source_sha256'] = manifest['provider_source_sha256']
        row['provider_abi'] = manifest['patch']['abi']
    # Fixture control r1 was not instrumented: native mode cannot silently use a
    # partly blackbox generation to validate original1W2R.
    with pytest.raises(ValueError, match='selection_count_differs'):
        evidence.validate_execution_mode(rows, config(path))


def test_worker_selection_manifest_path_must_be_canonical_run_copy(tmp_path, build_manifest):
    from r2f.instrumentation.fastdds_peer.receipt import validate_execution_mode
    from test_fastdds_peer_activation import reference
    path, manifest = build_manifest
    raw = tmp_path/'runtime/raw.jsonl'
    raw.parent.mkdir()
    copied = raw.parent/'native-peer-build-manifest.json'
    copied.write_bytes(path.read_bytes())
    token = dict(event='endpoint_created',event_index=2,logical_id='w0',generation=1,
        endpoint_generation='gen',pid=100,pgid=100,process_start_ticks=10,
        run_id='offline',domain_id=154,observation_mode='instrumentation')
    selected = dict(token,event='native_peer_provider_selected',event_index=1,
        manifest_reference=reference(copied),vendor_binary=manifest['binaries']['vendor'],
        provider_binary=manifest['binaries']['provider'],provider_abi=manifest['patch']['abi'],
        provider_source_sha256=manifest['provider_source_sha256'])
    rows=[selected,token]
    assert validate_execution_mode(rows,config(path),raw_path=raw)
    # Same bytes, wrong artifact path: no contradiction is silently accepted.
    selected['manifest_reference'] = reference(path)
    with pytest.raises(ValueError,match='actual_selection_differs_from_config'):
        validate_execution_mode(rows,config(path),raw_path=raw)
