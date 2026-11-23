"""Offline input -> existing reconciliation/schema seam. Literal captures only."""
from copy import deepcopy
import hashlib
import json
from pathlib import Path
import time

import pytest

from test_discovery_layers_observer import observe
from test_instrumentation_sedp_projection import fixture, hashed, jsonraw, pcap, BASE
from test_topic_wire import packet, frame
from r2f.instrumentation.sedp import reconcile_discovery_projection


def parts_with_layers(tmp_path):
    wire, graph, bindings, window = fixture(tmp_path)
    # Same finalized capture contains literal DCPSTopic and SPDP DATA. SPDP
    # participant GUID = 0102030405060708090a0b0c000001c1, lease=20s.
    spdp = bytes.fromhex('500010000102030405060708090a0b0c000001c1'
                         '02000800140000000000000001000000')
    capture = wire['capture_receipt']
    data = Path(wire['raw']['path']).read_bytes() + pcap([
        (BASE+125, frame(packet())),
        (BASE+126, frame(packet(spdp, writer='000100c2')))])[24:]
    capture['raw'] = hashed(tmp_path/'wire.pcap', data)
    from r2f.instrumentation.sedp import parse_sedp_capture
    # Set a broad capture lifecycle around the fake native provider. This is
    # fixture chronology, never execution evidence or a mature run.
    root = tmp_path/'native'
    root.mkdir()
    reference = observe(root).finish()
    capture['unix_ns'] = time.time_ns()+1000000
    capture['receipt_raw'] = jsonraw(tmp_path/'capture.json',
        {k:v for k,v in capture.items() if k != 'receipt_raw'})
    wire = parse_sedp_capture(capture)
    return wire, graph, bindings, window, reference


def reconcile(parts):
    wire, graph, bindings, window, layers = parts
    return reconcile_discovery_projection(wire, graph, endpoint_bindings=bindings,
        observation_window=window, discovery_layers=layers)


def test_hash_backed_builtin_and_same_capture_layers_reach_schema(tmp_path):
    parts = parts_with_layers(tmp_path)
    result = reconcile(parts)
    schema = result['schema_check']
    layers = schema['discovery_layers']
    assert layers['identity']['status'] == 'established'
    assert layers['participant']['builtin_observations'][0]['guid'] == '01'*12+'000001c1'
    assert layers['participant']['wire']['observations'][0]['participant_guid'] == '0102030405060708090a0b0c000001c1'
    assert layers['topic']['wire']['observations'][0]['fields']['depth']['value'] == 7
    assert layers['topic']['builtin_observations'][0]['topic_key'] == 'ab'*16
    assert layers['topic']['key_kind'] == 'opaque16_not_guid'
    assert layers['topic']['owner_generation'] == 'unknown'
    assert layers['participant']['api_lease_duration'] == 'unavailable_api'
    assert layers['participant']['process_generation'] == 'unknown'
    assert layers['topic']['wire']['raw']['sha256'] == parts[0]['raw']['sha256']
    assert schema['all_layer_obligations_closed'] is False
    # Auxiliary layer collection cannot change the existing endpoint oracle.
    legacy = reconcile_discovery_projection(parts[0], parts[1],
        endpoint_bindings=parts[2], observation_window=parts[3])
    assert result['comparison_status'] == legacy['comparison_status']
    assert result['comparisons'] == legacy['comparisons']


def test_raw_tamper_is_unknown_without_promoting_supplied_samples(tmp_path):
    parts = parts_with_layers(tmp_path)
    Path(parts[-1]['raw']['path']).write_text('{}')
    layers = reconcile(parts)['schema_check']['discovery_layers']
    assert layers['identity']['status'] == 'unknown'
    assert layers['participant']['builtin_observations'] == []
    assert layers['responsibility_layer'] == 'harness'


def test_profile_control_survives_capsule_and_changes_input_identity():
    from r2f.campaign.exploratory_input import apply_profile_controls, runtime_input
    from r2f.campaign.identity import semantic_input
    from r2f.core import ReplayCapsule, compile_scenario, canonical_hash
    from r2f.modules.s1.module import S1Module
    target = S1Module().targets['builtin.discovery_projection_reconciliation']
    scenario = compile_scenario(target, 'rmw_cyclonedds_cpp', 'baseline', 0)
    parent = semantic_input('baseline', target_id='builtin.discovery_projection_reconciliation')
    enabled = apply_profile_controls(parent, {'projection_discovery_layers': 'participant_topic_v1'},
                                     backend='projection_pair')
    capsule = ReplayCapsule.from_scenario(scenario, source_manifest={'head':'offline'}, final_input=enabled)
    restored = capsule.input['effective_input']['final_input']
    assert runtime_input(restored, backend='projection_pair')['projection_discovery_layers'] == 'participant_topic_v1'
    assert canonical_hash(enabled) != canonical_hash(parent)
    assert 'projection_discovery_layers' not in runtime_input(parent, backend='projection_pair')
    with pytest.raises(ValueError, match='unsupported'):
        apply_profile_controls(parent, {'projection_discovery_layers': 'other'}, backend='projection_pair')


def test_adapter_carries_auxiliary_layers_and_rejects_unbound_copy(tmp_path):
    from test_campaign_projection import projection_fixture
    from r2f.campaign.projection_adapter import projection_facts
    scenario, config, runtime, capsule = projection_fixture(tmp_path)
    config['projection_discovery_layers'] = 'participant_topic_v1'
    root = tmp_path/'native'
    root.mkdir()
    reference = observe(root).finish()
    # Fixture wall clocks differ deliberately: adapter must expose unknown
    # chronology, not drop the auxiliary receipt or synthesize a pass.
    runtime['discovery_projection']['discovery_layers'] = reference
    events = runtime['events']
    original = next(e for e in events if e['event'] == 'projection_observation')
    original['discovery_layers'] = deepcopy(reference)
    stopped = deepcopy(original)
    stopped.update(event='discovery_builtin_observer_stopped', receipt=reference)
    events.append(stopped)
    admission = next(e for e in events if e['event'] == 'namespace_host_admission_verified')
    native = json.loads(Path(reference['raw']['path']).read_bytes())
    native['admission'] = deepcopy(admission)
    native['domain_id'] = config['domain_id']
    native['pid'] = original['pid']
    native['receipts'][0]['receipt']['domain_id'] = config['domain_id']
    reference['raw'] = jsonraw(Path(reference['raw']['path']), native)
    runtime['discovery_projection']['discovery_layers'] = deepcopy(reference)
    original['discovery_layers'] = deepcopy(reference)
    stopped['receipt'] = deepcopy(reference)
    started = deepcopy(admission)
    started.update(event='discovery_builtin_observer_started', profile=native['profile'],
                   environment=native['environment'], admission_ref=admission['raw_event_ref'])
    events.append(started)
    facts = projection_facts(events, runtime, config)
    schema = facts['projection']['schema_check']
    assert schema['discovery_layers']['identity']['status'] == 'unknown'
    assert 'builtin_capture_chronology_mismatch' in schema['discovery_layers']['identity']['reasons']
    original['discovery_layers'] = {'raw': {'path': 'forged'}}
    facts = projection_facts(events, runtime, config)
    assert 'projection' not in facts
    assert 'raw observation differs' in facts['evidence_error']


def namespace_slice(tmp_path, monkeypatch, *, roles=None, native_failure=None, admission_failure=False, worker_failure=False):
    """Exercise production namespace/projection lifecycle with only OS/DDS seams fake."""
    import ctypes
    import socket
    from unittest.mock import Mock
    from test_projection_multi_endpoint import MultiRoleProjection, MultiRolePool
    from test_discovery_layers_observer import NativeProvider
    from r2f.runtime import namespace_pair, projection_pair, native_participant
    from r2f.instrumentation import sedp
    from r2f.runtime.discovery_profiles import write_profiles
    harness = MultiRoleProjection(tmp_path, roles=roles or ['writer', 'reader'])
    from r2f.campaign.exploratory_input import apply_profile_controls
    from r2f.campaign.identity import semantic_input, runtime_config
    final_input = apply_profile_controls(semantic_input('baseline', {'topic': harness.c['topic']},
        target_id=harness.c['target_id']), {'projection_discovery_layers': 'participant_topic_v1',
        'projection_roles': harness.c['projection_roles']}, backend='projection_pair')
    harness.c = runtime_config(final_input, run_id='offline-discovery-layers',
        target_id=harness.c['target_id'], target_revision=0, rmw='rmw_cyclonedds_cpp',
        arm='baseline', repeat=0, domain_id=19, mutation_kind='endpoint_qos_variation', carrier='ros2_endpoint')
    provider = NativeProvider(fail=native_failure)
    selected_environments = {}

    def forbidden(*args, **kwargs):
        raise AssertionError('real OS/DDS operation forbidden in offline slice')

    monkeypatch.setattr(ctypes, 'CDLL', forbidden)
    monkeypatch.setattr(socket, 'socket', forbidden)
    monkeypatch.setattr(projection_pair.time, 'time_ns', harness.clock.time_ns)
    monkeypatch.setattr(projection_pair.time, 'monotonic_ns', harness.clock.monotonic_ns)

    def admission(request):
        if admission_failure:
            raise RuntimeError('host_owner_channel_closed')
        return {'net_namespace': 'offline_authenticated_fixture'}

    monkeypatch.setattr(namespace_pair, 'validate_host_leases', admission)
    monkeypatch.setattr(namespace_pair, 'same_source', lambda *a: True)
    monkeypatch.setattr(namespace_pair, 'execution_chain_identity', lambda: {})
    monkeypatch.setattr(namespace_pair, 'probe_rtps_ports', lambda *a: {'ok': True})
    monkeypatch.setattr(namespace_pair, 'write_profiles', write_profiles)

    def factory():
        assert harness.ready['status'] == 'ready'
        return provider

    monkeypatch.setattr(native_participant, 'NativeParticipantBackend', factory)

    def pool(config, env, out, emit, **kwargs):
        harness.emit = emit
        selected_environments.update(deepcopy(kwargs['role_environments']))
        harness.pool = MultiRolePool(harness, config)
        if worker_failure:
            harness.pool.start = lambda role: (_ for _ in ()).throw(RuntimeError('fake_worker_start_failed'))
        return harness.pool

    monkeypatch.setattr(namespace_pair, 'OwnerCheckedPool', pool)
    monkeypatch.setattr(sedp, 'PassiveSedpObserver', lambda *a: harness)
    monkeypatch.setattr(sedp, 'build_capture_spec', lambda *a, **kw: {})
    monkeypatch.setattr(projection_pair, 'capture_libraries', lambda pid, ticks, path: jsonraw(path, {}))
    original_stop = harness.stop

    def stop():
        # Native close must precede capture stop, including native API failure.
        assert provider.closed
        harness.records.extend([(harness.clock.time_ns(), frame(packet())),
            (harness.clock.time_ns(), frame(packet(bytes.fromhex(
                '500010000102030405060708090a0b0c000001c1'
                '02000800140000000000000001000000'), writer='000100c2')))])
        harness.path.write_bytes(pcap(harness.records))
        return original_stop()

    monkeypatch.setattr(harness, 'stop', stop)
    events = []
    monkeypatch.setattr('builtins.print', lambda text, **kw: events.append(json.loads(text)))
    result = projection_pair.namespace_body({'config': harness.c, 'output_dir': str(tmp_path),
                                              'leases': [], 'source': {}}, Mock())
    for i, event in enumerate(events):
        event['event_index'] = i
    raw = hashed(tmp_path/'raw.jsonl', b''.join(json.dumps(e).encode()+b'\n' for e in events))
    result.update(events=events, raw_path=raw['path'], raw_sha256=raw['sha256'])
    projection_pair.finalize_projection(result)
    return harness, result, selected_environments, provider


@pytest.mark.parametrize('roles', [['writer', 'reader'], ['writer', 'reader', 'writer2', 'reader2']])
def test_opt_in_public_namespace_path_reaches_adapter_with_all_worker_profiles(tmp_path, monkeypatch, roles):
    from r2f.campaign.projection_adapter import projection_facts
    harness, result, environments, provider = namespace_slice(tmp_path, monkeypatch, roles=roles)
    assert result['runtime_status'] == 'completed', result['reason']
    assert provider.closed
    for role in roles+['builtin_observer']:
        assert '<EnableTopicDiscoveryEndpoints>true</EnableTopicDiscoveryEndpoints>' in Path(
            environments[role]['CYCLONEDDS_URI']).read_text()
    schema = projection_facts(result['events'], result, harness.c)['projection']['schema_check']
    layers = schema['discovery_layers']
    assert layers['identity']['status'] == 'established', layers
    assert layers['status'] == 'observed'
    assert layers['topic']['wire']['observations'][0]['fields']['depth']['value'] == 7
    assert layers['participant']['wire']['observations'][0]['participant_guid'] == '0102030405060708090a0b0c000001c1'
    assert layers['topic']['builtin_raw_refs'][0]['sha256'] == result['discovery_projection']['discovery_layers']['raw']['sha256']
    assert schema['all_layer_obligations_closed'] is False


def test_native_failure_stays_named_unknown_in_existing_projection(tmp_path, monkeypatch):
    from r2f.campaign.projection_adapter import projection_facts
    harness, result, _, provider = namespace_slice(tmp_path, monkeypatch, native_failure='DCPSTopic')
    assert result['runtime_status'] == 'completed', result['reason']
    assert provider.closed
    layers = projection_facts(result['events'], result, harness.c)['projection']['schema_check']['discovery_layers']
    assert layers['status'] == 'unknown'
    assert any('reader_failed' in reason for reason in layers['reasons'])
    assert layers['candidate_eligible'] is False


def test_failed_admission_never_constructs_native_or_profiles(tmp_path, monkeypatch):
    _, result, environments, provider = namespace_slice(tmp_path, monkeypatch, admission_failure=True)
    assert result['runtime_status'] == 'blocked'
    assert result['reason'] == 'host_owner_channel_closed'
    assert environments == {}
    assert not provider.closed  # never constructed/opened by the runtime
    assert not (tmp_path/'profiles').exists()
    assert not (tmp_path/'discovery-builtins.json').exists()


@pytest.mark.parametrize('damage,reason', [
    ('projected_samples', 'builtin_samples_not_reproduced_from_receipts'),
    ('loan', 'builtin_take_or_loan_unknown'),
    ('chronology', 'builtin_capture_chronology_mismatch'),
    ('capture_receipt', 'discovery_capture_receipt_not_reproduced'),
    ('malformed', 'raw_JSON_object_required'),
    ('profile', 'builtin_profile_identity_mismatch'),
])
def test_rehashed_or_malformed_evidence_does_not_qualify_layers(tmp_path, damage, reason):
    parts = parts_with_layers(tmp_path)
    raw_path = Path(parts[-1]['raw']['path'])
    native = json.loads(raw_path.read_bytes())
    if damage == 'projected_samples':
        native['samples']['DCPSTopic'][0]['history']['depth'] = 99
    elif damage == 'loan':
        row = next(r for r in native['receipts'] if r['operation'].startswith('take:'))
        row['receipt']['loan_returned'] = False
    elif damage == 'chronology':
        native['closed_unix_ns'] = parts[0]['capture_receipt']['unix_ns'] + 1
    elif damage == 'capture_receipt':
        parts[0]['capture_receipt']['ready']['unix_ns'] += 1
    elif damage == 'malformed':
        native = []
    elif damage == 'profile':
        Path(native['profile']['path']).write_text('<CycloneDDS/>')
    if damage != 'capture_receipt':
        parts[-1]['raw'] = jsonraw(raw_path, native)
    layers = reconcile(parts)['schema_check']['discovery_layers']
    assert layers['identity']['status'] == 'unknown'
    assert reason in layers['identity']['reasons'][0]
    assert layers['topic']['builtin_observations'] == []
    assert layers['all_layer_obligations_closed'] is False


def test_unqualified_clocks_do_not_publish_a_correlated_time_delta(tmp_path):
    layers = reconcile(parts_with_layers(tmp_path))['schema_check']['discovery_layers']
    assert layers['timing']['status'] == 'unknown'
    assert layers['topic']['wire']['observations'][0]['native_close_minus_wire_ns'] is None


@pytest.mark.parametrize('damage,reason', [
    ('reader_handle', 'builtin_take_reader_identity_mismatch'),
    ('reader_topic', 'builtin_reader_identity_mismatch'),
    ('reader_reused', 'builtin_reader_identity_mismatch'),
    ('open_rejected', 'builtin_open_not_accepted'),
    ('open_domain', 'builtin_open_domain_mismatch'),
    ('admission_domain', 'builtin_admission_domain_mismatch'),
])
def test_native_identity_is_bound_to_open_reader_admission_and_domain(tmp_path, damage, reason):
    parts = parts_with_layers(tmp_path)
    path = Path(parts[-1]['raw']['path'])
    native = json.loads(path.read_bytes())
    opened = next(r['receipt'] for r in native['receipts'] if r['operation'] == 'open')
    readers = [r['receipt'] for r in native['receipts'] if r['operation'].startswith('reader:')]
    take = next(r['receipt'] for r in native['receipts'] if r['operation'].startswith('take:'))
    if damage == 'reader_handle':
        take['reader_handle'] = 999
    elif damage == 'reader_topic':
        readers[0]['topic'] = 'DCPSTopic'
    elif damage == 'reader_reused':
        readers[1]['reader_handle'] = readers[0]['reader_handle']
    elif damage == 'open_rejected':
        opened['accepted'] = False
    elif damage == 'open_domain':
        opened['domain_id'] = 999
    elif damage == 'admission_domain':
        native['admission']['domain_id'] = 999
    parts[-1]['raw'] = jsonraw(path, native)
    layers = reconcile(parts)['schema_check']['discovery_layers']
    assert layers['identity']['status'] == 'unknown'
    assert reason in layers['identity']['reasons'][0]


def test_observed_samples_without_instance_association_are_not_reconciled(tmp_path):
    layers = reconcile(parts_with_layers(tmp_path))['schema_check']['discovery_layers']
    assert layers['participant']['association']['status'] == 'unknown'
    assert 'unmatched' in layers['participant']['association']['reasons'][0]
    assert layers['topic']['association']['status'] == 'unknown'
    assert layers['topic']['reconciliation_status'] == 'unknown'
    assert layers['participant']['reconciliation_status'] == 'unknown'
    assert layers['all_layer_obligations_closed'] is False


def test_duplicate_native_participant_identity_is_separate_unknown(tmp_path):
    parts = parts_with_layers(tmp_path)
    path = Path(parts[-1]['raw']['path'])
    raw = json.loads(path.read_bytes())
    raw['samples']['DCPSParticipant'] *= 2
    take = next(r['receipt'] for r in raw['receipts'] if r['operation'] == 'take:DCPSParticipant')
    take['samples'] *= 2
    take['take_return_code'] = 2
    parts[-1]['raw'] = jsonraw(path, raw)
    participant = reconcile(parts)['schema_check']['discovery_layers']['participant']
    assert participant['association']['status'] == 'unknown'
    assert 'participant_builtin_identity_ambiguous' in participant['association']['reasons']
    assert participant['reconciliation_status'] == 'unknown'


def test_canonical_domain_cannot_be_rebound_by_auxiliary_raw(tmp_path, monkeypatch):
    from r2f.campaign.projection_adapter import projection_facts
    harness, result, _, _ = namespace_slice(tmp_path, monkeypatch)
    reference = result['discovery_projection']['discovery_layers']
    native = json.loads(Path(reference['raw']['path']).read_bytes())
    native['domain_id'] = 999
    reference['raw'] = jsonraw(Path(reference['raw']['path']), native)
    next(e for e in result['events'] if e['event'] == 'projection_observation')['discovery_layers'] = deepcopy(reference)
    next(e for e in result['events'] if e['event'] == 'discovery_builtin_observer_stopped')['receipt'] = deepcopy(reference)
    facts = projection_facts(result['events'], result, harness.c)
    assert 'canonical admission/domain differs' in facts['evidence_error']
    assert 'projection' not in facts


def test_abort_after_native_start_closes_and_emits_raw_receipt(tmp_path, monkeypatch):
    _, result, _, provider = namespace_slice(tmp_path, monkeypatch, worker_failure=True)
    assert result['reason'] == 'fake_worker_start_failed'
    assert provider.closed
    stopped = next(e for e in result['events'] if e['event'] == 'discovery_builtin_observer_stopped')
    data = Path(stopped['receipt']['raw']['path']).read_bytes()
    assert hashlib.sha256(data).hexdigest() == stopped['receipt']['raw']['sha256']
    assert json.loads(data)['cleanup_complete'] is True
    capture_stop = next(e for e in result['events'] if e['event'] == 'discovery_observer_stopped')
    assert stopped['event_index'] < capture_stop['event_index']


def test_opt_in_rejects_non_cyclone_and_observation_payload_controls():
    from r2f.campaign.exploratory_input import apply_profile_controls
    from r2f.campaign.identity import semantic_input, runtime_config
    parent = semantic_input('baseline', target_id='builtin.discovery_projection_reconciliation')
    enabled = apply_profile_controls(parent, {'projection_discovery_layers': 'participant_topic_v1'},
                                     backend='projection_pair')
    with pytest.raises(ValueError, match='unsupported_projection_discovery_layers_rmw'):
        runtime_config(enabled, target_id='builtin.discovery_projection_reconciliation',
            rmw='rmw_fastrtps_cpp', arm='baseline', run_id='offline', domain_id=19,
            mutation_kind='endpoint_qos_variation', carrier='ros2_endpoint')
    with pytest.raises(ValueError, match='unsupported|unknown'):
        apply_profile_controls(parent, {'discovery_layers': {'samples': ['configured']}},
                               backend='projection_pair')
