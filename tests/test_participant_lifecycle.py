"""Pinned Cyclone log grammar: deterministic boundaries, never DDS evidence."""
from r2f.instrumentation.participant_lease import parse_lifecycle_trace

GUID = '01100102030405060708090a000001c1'
VENDOR_GUID = '1100102:3040506:708090a:1c1'


def test_lifecycle_trace_keeps_refresh_expiry_and_gc_separate():
    raw = (f'1795500000.000000 [173] dq.builtin: SPDP ST0 {VENDOR_GUID} bes fc3f NEW\n'
           f'1795500001.000000 [173] dq.builtin: SPDP ST0 {VENDOR_GUID} (known) L(:1c1 42.000000)\n'
           f'1795500012.000000 [173]       tev: lease expired: l 0x123 guid {VENDOR_GUID} tend 40 < now 41\n'
           f'1795500012.001000 [173]        gc: gc_delete_proxy_participant(0x345, {VENDOR_GUID})\n').encode()
    result = parse_lifecycle_trace(raw, domain_id=173)
    assert [e['kind'] for e in result['events']] == ['discovered', 'refreshed', 'expired', 'gc_deleted']
    assert {e['participant_guid'] for e in result['events']} == {GUID}
    assert result['events'][2]['expiry_deadline_elapsed_ns'] == 40
    assert result['events'][2]['expiry_observed_elapsed_ns'] == 41
    assert result['events'][3]['offset'] == raw.index(b'1795500012.001000')
    assert result['candidate_eligible'] is False
    assert result['race_status'] == 'not_established'


def test_known_spdp_and_writer_lease_never_prove_participant_refresh_or_expiry():
    raw = (f'1795500001.000000 [173] dq.builtin: SPDP ST0 {VENDOR_GUID} (known)\n'
           '1795500002.000000 [173] tev: lease expired: l 0x1 guid 1100102:3040506:708090a:103 tend 40 < now 41\n'
           f'1795500003.000000 [174] gc: gc_delete_proxy_participant(0x1, {VENDOR_GUID})\n'
           f'1795500004.000000 [173] tev: lease expired: l 0x1 guid {VENDOR_GUID} tend 41 < now 40\n'
           f'1795500005.000000 [173] gc: gc_delete_proxy_participant(0x1, {VENDOR_GUID})').encode()
    result = parse_lifecycle_trace(raw, domain_id=173)
    assert result['events'] == []
    assert [d['reason'] for d in result['diagnostics']] == [
        'lifecycle_domain_or_entity_mismatch',
        'lifecycle_expiry_order_invalid', 'lifecycle_trace_truncated_line']
    assert len(result['out_of_scope_events']) == 1


def test_canonical_recovery_report_names_missing_lease_evidence_without_claiming_expiry(tmp_path):
    from test_recovery_trajectory_admission import OfflineRecoveryTrajectory
    from r2f.modules.s1.oracles import evaluate_primary
    fixture = OfflineRecoveryTrajectory(tmp_path)
    fixture.config['observe_participant_leases'] = True
    facts = fixture.facts()
    lease = facts['participant_lease']
    assert lease['status'] == 'unknown'
    assert lease['expiry_status'] == 'unknown'
    assert lease['cleanup_status'] == 'unknown'
    assert 'participant_ownership_unavailable:writer' in lease['reasons']
    assert lease['candidate_eligible'] is False
    assert evaluate_primary(facts, 'discovery.interruption_recovery')['verdict'] == 'pass'


def test_public_campaign_selects_one_lease_recovery_cell_and_preserves_the_full_matrix(tmp_path):
    import json
    from r2f.campaign.runner import run_campaign
    calls = []
    def fixture(config, directory):
        calls.append(config)
        return {'runtime_status': 'blocked', 'reason': 'offline_lease_fixture', 'real_execution': False}
    report = run_campaign(tmp_path, execute=True, runtime=fixture, max_executions=1,
        target_id='discovery.interruption_recovery', parameters={'observe_participant_leases': True},
        observation={'observation_mode': 'instrumentation', 'observe_adopted_config': False,
                     'cell_id': 'rmw_cyclonedds_cpp.mutated.1'})
    assert len(calls) == 1
    assert (calls[0]['arm'], calls[0]['repeat'], calls[0]['observation_mode']) == ('mutated', 1, 'instrumentation')
    assert len(report['rows']) == 24
    assert report['full_scope_required_cells'] == 72
    assert sum(r['execution_stage'] == 'planned' for r in report['rows']) == 23
    from pathlib import Path
    for row in report['rows']:
        config = json.loads(Path(row['runtime_config_path']).read_text())
        assert ('observation_mode' in config) == (row['cell_id'] == 'rmw_cyclonedds_cpp.mutated.1')


def test_lifecycle_trace_configuration_uses_cyclone_native_unprefixed_element_names(tmp_path):
    from r2f.instrumentation.cyclonedds import build_trace_config
    from r2f.runtime.discovery_profiles import CYCLONE_XML
    config = build_trace_config(tmp_path / 'lease.log', domain_id=173,
        base_xml=CYCLONE_XML, observe_participant_lifecycle=True)
    assert '<CycloneDDS>' in config.xml
    assert 'ns0:' not in config.xml
    assert '<SPDPInterval>1s</SPDPInterval>' in config.xml
    assert '<Category>discovery,plist,trace</Category>' in config.xml


def lease_fixture(tmp_path, kinds=('discovered', 'refreshed', 'expired', 'gc_deleted')):
    """Synthetic I/O at the existing recovery capture seam, never an execution."""
    import hashlib
    import struct
    from test_recovery_trajectory_admission import OfflineRecoveryTrajectory
    from test_spdp_participant_lease import capture, data_packet
    from r2f.instrumentation.cyclonedds import build_trace_config
    fixture = OfflineRecoveryTrajectory(tmp_path)
    fixture.config.update(observe_participant_leases=True, observation_mode='instrumentation',
                          target_id='discovery.interruption_recovery', target_revision=0,
                          repeat=0, effective_input_sha256='offline-input-digest')
    guids = {}
    for role, event in fixture.tokens.items():
        guid = event['endpoint_gid'][:24] + '000001c1'
        guids[role] = guid
        event.update(run_id=fixture.config['run_id'], domain_id=80, process_start_ticks=91,
                     create_begin_monotonic_ns=event['monotonic_ns'] - 10)
        event['participant_ownership'] = dict(schema='r2f.cyclone-participant-ownership/v2',
            complete=True, status='observed', domain_id=80, role_kind=role,
            local_gid=event['endpoint_gid'], participant_gid=guid, provenance={'offline_fixture': True},
            stable_sample_count=2, participant_handle=1, local_handle=2,
            started_monotonic_ns=event['monotonic_ns']-9, ended_monotonic_ns=event['monotonic_ns']-1,
            participant_root_observation={'participant_handle': 1, 'participant_self_return': 1,
                                          'lookup_participants': [1]},
            entity_identities=[{'handle': 1, 'gid': guid}, {'handle': 2, 'gid': event['endpoint_gid']}])
    for event in fixture.events:
        if event.get('role') in fixture.tokens:
            event.update(run_id=fixture.config['run_id'], domain_id=80, process_start_ticks=91)
    packets = [data_packet(parameters=bytes.fromhex('50001000'+g+'020008000a0000000000000001000000'),
                           prefix=g[:24]) for g in guids.values()]
    artifact = capture(tmp_path, packets)
    fixture.emit('discovery_observer_stopped', receipt={'raw': artifact['raw'], 'health': artifact['health']})
    metadata = fixture.emit('participant_lease_execution_metadata', observation_mode='instrumentation',
        trace_configs={}, effective_input_sha256='offline-input-digest',
        run_id=fixture.config['run_id'], domain_id=80)
    for role, remote in (('writer', 'reader'), ('reader', 'writer')):
        guid = guids[remote]
        vendor_guid = ':'.join(f'{int(guid[i:i+8],16):x}' for i in range(0,32,8))
        text = ''
        start = fixture.tokens[remote]['time_ns'] + 10_000_000
        expiry = fixture.intermediate[role]['request']['time_ns'] - 10_000_000
        times = {'discovered': start, 'refreshed': start+10_000_000,
                 'expired': expiry, 'gc_deleted': expiry+1_000_000}
        for index, kind in enumerate(kinds):
            body = {'discovered': f'SPDP ST0 {vendor_guid} bes fc3f NEW',
                    'refreshed': f'SPDP ST0 {vendor_guid} (known) L(:1c1 12.000000)',
                    'expired': f'lease expired: l 0x1 guid {vendor_guid} tend 1 < now 2',
                    'gc_deleted': f'gc_delete_proxy_participant(0x2, {vendor_guid})',
                    'writer_expired': 'lease expired: l 0x3 guid 1100102:3040506:708090a:103 tend 1 < now 2'}[kind]
            stamp = times.get(kind, expiry)
            text += f'{stamp/1e9:016.6f} [80] dq.builtin: {body}\n'
        path = tmp_path / (role+'.trace.log')
        path.write_text(text)
        trace = build_trace_config(tmp_path / (role+'.prepared'), domain_id=80,
                                   observe_participant_lifecycle=True).to_dict()
        trace['output_file'] = str(path)
        metadata['trace_configs'][role] = trace
        libraries=[]
        for name in ('libddsc.so','librmw_cyclonedds_cpp.so'):
            lib=tmp_path/name
            lib.write_bytes(b'offline-library-fixture')
            libraries.append({'mapped_path': str(lib), 'path': str(lib), 'identity_verified': True,
                              'sha256':hashlib.sha256(lib.read_bytes()).hexdigest()})
        process=dict(scope='observed_process', pid=fixture.tokens[role]['pid'], process_start_ticks=91,
            process_identity_stable=True, vendor_loaded=True, config_matches=True, domain_matches=True,
            rmw_matches=True, trace_fd_attached=True, trace_inode=path.stat().st_ino,
            trace_device=path.stat().st_dev, loaded_libraries=libraries,
            selected_environment={'CYCLONEDDS_URI':trace['xml'], 'ROS_DOMAIN_ID':'80',
                                  'RMW_IMPLEMENTATION':'rmw_cyclonedds_cpp'})
        fixture.emit('participant_lease_trace_closed', worker_role=role, config=trace,
            raw_path=str(path), raw_sha256=hashlib.sha256(path.read_bytes()).hexdigest(),
            processes=[process], vendor_process_verified=True,
            run_id=fixture.config['run_id'], domain_id=80)
    return fixture


def test_empty_internal_trace_remains_unknown_at_the_recovery_report_seam(tmp_path):
    fixture = lease_fixture(tmp_path, kinds=())
    result = fixture.facts()['participant_lease']
    assert result.get('internal_lifecycle_status') != 'observed'
    assert result['status'] == 'unknown'
    assert 'participant_lifecycle_events_missing:writer' in result['reasons']


def test_expiry_after_gc_is_unknown_not_a_collected_expiry(tmp_path):
    fixture = lease_fixture(tmp_path, kinds=('discovered', 'gc_deleted', 'expired'))
    # Keep timestamps increasing so this tests lifecycle order, not clock order.
    import hashlib
    for event in fixture.events:
        if event['event'] != 'participant_lease_trace_closed':
            continue
        from pathlib import Path
        path=Path(event['raw_path'])
        raw=path.read_bytes().splitlines(keepends=True)
        raw[2]=raw[2].replace(raw[2].split(b' ',1)[0],raw[1].split(b' ',1)[0])
        path.write_bytes(b''.join(raw))
        event['raw_sha256']=hashlib.sha256(path.read_bytes()).hexdigest()
    result = fixture.facts()['participant_lease']
    assert result['expiry_status'] == 'unknown'
    assert result['cleanup_status'] == 'unknown'
    assert 'participant_expiry_after_retirement' in result['reasons']


def test_writer_liveliness_expiry_does_not_discard_independent_participant_facts(tmp_path):
    fixture = lease_fixture(tmp_path, kinds=('discovered','refreshed','writer_expired','expired','gc_deleted'))
    result = fixture.facts()['participant_lease']
    assert result['internal_lifecycle_status'] == 'observed'
    assert result['expiry_status'] == 'observed'
    assert result['cleanup_status'] == 'observed'
    assert not result['reasons']


def test_public_removal_rejects_pre_expiry_match_and_wrong_generation_ack(tmp_path):
    fixture = lease_fixture(tmp_path)
    result=fixture.facts()['participant_lease']
    assert all(row['public_removal_status']=='observed' for row in result['external_correlation'])
    for role in ('writer','reader'):
        fixture.intermediate[role]['matches']['time_ns'] = fixture.tokens[role]['time_ns']
        fixture.intermediate[role]['ack']['endpoint_generation'] = 'retired-generation'
    result=fixture.facts()['participant_lease']
    assert all(row['public_removal_status']=='unknown' for row in result['external_correlation'])


def test_delayed_emission_cannot_turn_pre_expiry_acquisition_into_fresh_removal(tmp_path):
    fixture = lease_fixture(tmp_path)
    for role in ('writer','reader'):
        sample = fixture.intermediate[role]
        sample['request']['requested_monotonic_ns'] = fixture.tokens[role]['created_monotonic_ns']
        sample['matches']['sampled_monotonic_ns'] = sample['request']['requested_monotonic_ns'] + 1
    result = fixture.facts()['participant_lease']
    assert all(row['public_removal_status']=='unknown' for row in result['external_correlation'])
