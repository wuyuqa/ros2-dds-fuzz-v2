"""Third-target contracts; offline fixtures never count as ROS evidence."""
import copy
import json
from pathlib import Path
import tempfile
import unittest

from r2f.core import canonical_hash
from r2f.modules.s1.module import S1Module
from r2f.modules.s1.targets import EXECUTION_ORDER

THIRD = EXECUTION_ORDER[2]


class RecoveryDeclarations(unittest.TestCase):
    def test_sealed_first_two_target_manifests_are_unchanged(self):
        expected = ('c67eb5837a34bb6970d4d59bf2cb339d1b6a07243af9d0ee949dbdc789533940',
                    '1f65b4dbfc7bc5c9be454251fa536705597c75a36981f4e23ba9e6e60076002d')
        module = S1Module()
        for target_id, digest in zip(EXECUTION_ORDER[:2], expected):
            self.assertEqual(canonical_hash(module.targets[target_id].to_dict()), digest)

    def test_only_real_pre_drop_actions_are_core_preconditions(self):
        target = S1Module().targets[THIRD].to_dict()
        names = ['create_endpoints', 'establish_match', 'verify_namespace_bindings']
        self.assertEqual([s['step_id'] for s in target['state_steps']], names)
        self.assertEqual([s['fact'] for s in target['state_preconditions']], names)
        self.assertEqual([r['fact'] for r in target['state_steps'][0]['requires']], ['environment_ok', 'locks_held', 'namespace_ready'])
        self.assertEqual(target['target_revision'], 0)
        self.assertEqual(target['rule_contract']['primary_rule'], 'discovery.recovery.resume_delivery')
        self.assertEqual(target['control_contract']['fault']['receipt_kind'], 'delivery_omission')
        self.assertEqual(target['control_contract']['fault']['window'], 'post_restore_delivery')


class RecoveryPlanning(unittest.TestCase):
    def test_capsule_semantics_reach_discovery_runtime_without_default_replacement(self):
        from r2f.campaign.runner import plan_campaign
        from r2f.core.replay import ReplayCapsule
        with tempfile.TemporaryDirectory() as temp:
            report = plan_campaign(temp, run_id='recovery-plan', domain_id=80, target_id=THIRD,
                                   parameters={'drop_duration_sec': 1.75, 'recovery_timeout_sec': 9.0,
                                               'counter_interval_sec': 0.125, 'post_match_count': 8})
            self.assertEqual(len(report['rows']), 24)
            self.assertEqual(report['full_scope_required_cells'], 72)
            digests = {}
            for row in report['rows']:
                config = json.loads(Path(row['runtime_config_path']).read_text())
                capsule = ReplayCapsule.from_dict(json.loads(Path(row['replay_path']).read_text()))
                self.assertIsNone(capsule.execution)
                self.assertEqual(row['execution_stage'], 'planned')
                final = capsule.input['effective_input']['final_input']
                for key, value in final.items(): self.assertEqual(config[key], value)
                self.assertEqual(config['mutation_parameters']['drop_duration_sec'], 1.75)
                self.assertEqual(config['recovery_timeout_sec'], 9.0)
                self.assertEqual(config['counter_interval_sec'], 0.125)
                self.assertEqual(config['post_match_count'], 8)
                self.assertEqual(config['carrier'], 'wire_proxy')
                self.assertEqual(config['mutation_kind'], 'discovery_drop')
                digests[row['arm']] = canonical_hash(final)
                if row['arm'] == 'negative':
                    self.assertEqual(row['fault_id'], 'post_restore_delivery_omission')
                    self.assertEqual(config['fault_id'], row['fault_id'])
            self.assertEqual(digests['baseline'], digests['transparent'])

    def test_preflight_cause_precedes_missing_steps_and_retains_raw_verification(self):
        from r2f.campaign.runner import plan_campaign
        from r2f.campaign.runtime_adapter import ingest_execution
        from r2f.core import ReplayCapsule, Scenario, raw_sha256
        with tempfile.TemporaryDirectory() as temp:
            plan = plan_campaign(temp, run_id='blocked-plan')
            cell = plan['rows'][0]
            config = json.loads(Path(cell['runtime_config_path']).read_text())
            capsule = ReplayCapsule.from_dict(json.loads(Path(cell['replay_path']).read_text()))
            reason = 'domain_conflict:rtps_port_block'
            event = {'event': 'runtime_end', 'event_index': 0, 'run_id': config['run_id'],
                     'domain_id': config['domain_id'], 'reason': reason, 'real_execution': False}
            raw = Path(temp) / 'blocked.jsonl'
            raw.write_text(json.dumps(event) + '\n')
            runtime = {'runtime_status': 'blocked', 'real_execution': False, 'reason': reason,
                       'raw_path': str(raw), 'raw_sha256': raw_sha256(raw), 'events': [event],
                       'environment': {'ok': False, 'reason': reason}}
            scenario = Scenario.from_dict(capsule.input['scenario'])
            for mode in ('valid', 'missing', 'damaged'):
                item = copy.deepcopy(runtime)
                if mode == 'missing': item.update(raw_path=None, raw_sha256=None)
                if mode == 'damaged': item['raw_sha256'] = '0' * 64
                row = ingest_execution(scenario, config, item, capsule)
                self.assertEqual(row['abort_reason'], reason)
                self.assertEqual(row['runtime_reason'], reason)
                self.assertEqual(row['execution_stage'], 'aborted')
                self.assertEqual(row['oracle_verdict'], 'unknown')
                self.assertEqual(row['environment_raw_evidence']['verified'], mode == 'valid')
                self.assertNotIn('ingestion_error', row)


def recovery_preconditions():
    """Offline receipt fixture: never an execution or namespace admission claim."""
    from r2f.core import compile_scenario
    scenario = compile_scenario(S1Module().targets[THIRD], 'rmw_cyclonedds_cpp', 'mutated', 0)
    events = []
    def emit(name, **fields):
        index = len(events)
        event = {'event': name, 'event_index': index, 'raw_event_ref': f'offline#{index}',
                 'run_id': 'offline-recovery', 'domain_id': 80, 'pid': 123,
                 'monotonic_ns': 1000 + 100 * index, 'time_ns': 100000 + 100 * index, **copy.deepcopy(fields)}
        events.append(event)
        return event
    emit('environment_preflight', ok=True)
    emit('isolation_locks_acquired', locks=['registry', 'domain', 'state'])
    isolation = {'isolated': True, 'interfaces': ['lo'], 'net_namespace': 'net:[2]',
                 'user_namespace': 'user:[2]', 'observed_net_namespace': 'net:[2]',
                 'observed_user_namespace': 'user:[2]', 'host_net_namespace': 'net:[1]',
                 'host_user_namespace': 'user:[1]', 'host_uid': 1000, 'uid_map': [[0, 1000, 1]],
                 'euid': 0, 'cap_eff': (1 << 12) | (1 << 13), 'host_cap_eff': 0}
    emit('namespace_host_admission_verified', isolation=isolation)
    ruleset = {'nftables': []}
    command = {'argv': ['nft', '-j', 'list', 'ruleset'], 'returncode': 0,
               'net_namespace': isolation['net_namespace'], 'started_monotonic_ns': 1210,
               'ended_monotonic_ns': 1220, 'stdout': json.dumps(ruleset)}
    emit('discovery_network_setup', receipt={'health_ok': True, 'drop_installed': False, 'hit': False,
         'counter_observed': False, 'isolation': isolation, 'initial_nft': ruleset,
         'command': command, 'monotonic_ns': 1230})
    emit('discovery_observer_ready', receipt={'status': 'ready', 'run_id': 'offline-recovery',
         'domain_id': 80, 'monotonic_ns': 1350, 'process': {'pid': 123}, 'interface': 'lo',
         'linktype': 1, 'timestamp_clock': 'kernel_SO_TIMESTAMPNS_realtime', 'spec_sha256': 'a' * 64,
         'namespace': {**isolation, 'loopback_flags': 9}})
    actuals = []
    for step in scenario.state_steps:
        actual = {'step_id': step.step_id, 'requires': [r.condition_id for r in step.requires],
                  'receipt_kind': 'precondition', 'satisfied': False,
                  'before': {'monotonic_ns': 1000 + 100 * len(events) - 1, 'satisfied': False},
                  'after': None}
        before = emit('state_step_before', **actual)
        actual['before']['raw_event_ref'] = before['raw_event_ref']
        actual['satisfied'] = True
        actual['evidence'] = {'offline_step': step.step_id}
        actual['after'] = {'monotonic_ns': before['monotonic_ns'] + 1, 'satisfied': True}
        after = emit('state_step_after', **actual)
        actual['after']['raw_event_ref'] = after['raw_event_ref']
        actuals.append(actual)
    snapshot = emit('discovery_precondition_snapshot', satisfied=True, precondition_receipts=actuals)
    emit('mutation_begin', precondition_snapshot_ref=snapshot['raw_event_ref'])
    return scenario, {'precondition_receipts': actuals}, events


class NamespaceReadinessAudit(unittest.TestCase):
    def test_namespace_ready_is_supported_by_three_prior_raw_receipts(self):
        from r2f.campaign.recovery_adapter import recovery_receipts
        scenario, runtime, events = recovery_preconditions()
        receipts = recovery_receipts(scenario, runtime, events)
        self.assertEqual(len(receipts), 3)
        first = receipts[0]
        self.assertTrue(first['before']['namespace_ready'])
        self.assertTrue(first['requires']['namespace_ready'])
        proof = first['result']['namespace_ready_evidence']
        self.assertEqual(set(proof), {'namespace_host_admission_verified', 'discovery_network_setup',
                                      'discovery_observer_ready'})
        for kind, reference in proof.items():
            self.assertEqual(events[reference['event_index']]['event'], kind)
            self.assertEqual(events[reference['event_index']]['raw_event_ref'], reference['raw_event_ref'])
            self.assertLess(reference['event_index'], first['result']['raw_before_index'])

    def test_absent_or_duplicate_readiness_evidence_cannot_set_namespace_ready(self):
        from r2f.campaign.recovery_adapter import recovery_receipts
        for kind in ('namespace_host_admission_verified', 'discovery_network_setup', 'discovery_observer_ready'):
            for mode in ('missing', 'duplicate'):
                with self.subTest(kind=kind, mode=mode):
                    scenario, runtime, events = recovery_preconditions()
                    event = next(e for e in events if e['event'] == kind)
                    if mode == 'missing': events.remove(event)
                    else: events.append(copy.deepcopy(event))
                    with self.assertRaises(ValueError): recovery_receipts(scenario, runtime, events)

    def test_readiness_must_precede_start_of_create_not_only_its_completion(self):
        from r2f.campaign.recovery_adapter import recovery_receipts
        for kind in ('namespace_host_admission_verified', 'discovery_network_setup', 'discovery_observer_ready'):
            for dimension in ('event_index', 'monotonic_ns'):
                with self.subTest(kind=kind, dimension=dimension):
                    scenario, runtime, events = recovery_preconditions()
                    event = next(e for e in events if e['event'] == kind)
                    create = next(e for e in events if e['event'] == 'state_step_before')
                    event[dimension] = create[dimension] + 1
                    with self.assertRaisesRegex(ValueError, 'before create_endpoints'):
                        recovery_receipts(scenario, runtime, events)

    def test_unhealthy_foreign_or_unready_receipts_do_not_prove_namespace(self):
        from r2f.campaign.recovery_adapter import recovery_receipts
        changes = [
            ('discovery_network_setup', ('health_ok',), False),
            ('discovery_network_setup', ('drop_installed',), True),
            ('discovery_network_setup', ('isolation', 'net_namespace'), 'net:[other]'),
            ('discovery_network_setup', ('command', 'returncode'), 1),
            ('discovery_network_setup', ('command', 'net_namespace'), 'net:[1]'),
            ('discovery_network_setup', ('command', 'stdout'), '{"nftables":[{"table":{}}]}'),
            ('discovery_network_setup', ('monotonic_ns',), 2000),
            ('discovery_observer_ready', ('status',), 'starting'),
            ('discovery_observer_ready', ('namespace', 'net_namespace'), 'net:[1]'),
            ('discovery_observer_ready', ('namespace', 'cap_eff'), 1 << 12),
            ('discovery_observer_ready', ('namespace', 'loopback_flags'), 8),
            ('discovery_observer_ready', ('monotonic_ns',), 2000),
            ('discovery_observer_ready', ('domain_id',), 81),
            ('discovery_observer_ready', ('process', 'pid'), 124),
        ]
        for kind, keys, value in changes:
            with self.subTest(kind=kind, keys=keys):
                scenario, runtime, events = recovery_preconditions()
                target = next(e for e in events if e['event'] == kind)['receipt']
                for key in keys[:-1]: target = target[key]
                target[keys[-1]] = value
                with self.assertRaises(ValueError): recovery_receipts(scenario, runtime, events)

    def test_runtime_requires_must_match_namespace_dependency(self):
        from r2f.campaign.recovery_adapter import recovery_receipts
        scenario, runtime, events = recovery_preconditions()
        runtime['precondition_receipts'][0]['requires'].remove('namespace_ready')
        with self.assertRaisesRegex(ValueError, 'identity/requires mismatch'):
            recovery_receipts(scenario, runtime, events)


def recovery_primary_facts(active=True):
    from test_s1_contracts import valid_facts
    facts = valid_facts()
    facts['drop'] = {'active': active, 'configured': active, 'applied': active, 'hit': active,
                     'pair_bound': True, 'counter_observed': active, 'packets': 12 if active else 0}
    facts['recovery'] = {'boundary_ns': 24, 'activity_ns': 26, 'rules_restored': True,
                         'identity_stable': True, 'activity_identity_correlated': True,
                         'fresh_match_and_graph': True, 'match_continuity': 'retained'}
    facts['observed_effects'] = ['discovery_temporarily_unavailable', 'discovery_restored']
    return facts


class RecoveryPrimaryOracle(unittest.TestCase):
    def test_retained_match_requires_real_recovery_activity_not_only_delivery(self):
        from r2f.modules.s1.oracles import evaluate_primary
        facts = recovery_primary_facts()
        self.assertEqual(evaluate_primary(facts, THIRD)['verdict'], 'pass')
        for field in ('rules_restored', 'activity_identity_correlated', 'fresh_match_and_graph'):
            with self.subTest(field=field):
                invalid = copy.deepcopy(facts)
                invalid['recovery'][field] = False
                self.assertEqual(evaluate_primary(invalid, THIRD)['verdict'], 'unknown')

    def test_middle_gap_fail_tail_unknown_and_unmatched_unknown(self):
        from r2f.modules.s1.oracles import evaluate_primary
        middle = recovery_primary_facts()
        middle['received'].pop(1)
        result = evaluate_primary(middle, THIRD)
        self.assertEqual(result['verdict'], 'fail')
        self.assertEqual(result['rule_id'], 'discovery.recovery.resume_delivery')
        self.assertIn('post_restore_delivery_loss', result['forbidden_residuals'])
        self.assertEqual(result['closure']['proven_gaps'], [1])
        tail = recovery_primary_facts()
        tail['received'].pop()
        result = evaluate_primary(tail, THIRD)
        self.assertEqual(result['verdict'], 'unknown')
        self.assertEqual(result['closure']['tail_inflight'], [2])
        unmatched = recovery_primary_facts()
        unmatched['match'] = None
        self.assertEqual(evaluate_primary(unmatched, THIRD)['verdict'], 'unknown')

    def test_stale_activity_and_zero_counter_never_become_sut_violation(self):
        from r2f.modules.s1.oracles import evaluate_primary
        for activity in (23, 24, 31, True):
            facts = recovery_primary_facts()
            facts['recovery']['activity_ns'] = activity
            self.assertEqual(evaluate_primary(facts, THIRD)['verdict'], 'unknown')
        for key, value in (('packets', 0), ('packets', True), ('pair_bound', False), ('hit', False)):
            facts = recovery_primary_facts()
            facts['drop'][key] = value
            self.assertEqual(evaluate_primary(facts, THIRD)['verdict'], 'unknown')

    def test_allowed_drop_effect_does_not_swallow_postrestore_residual(self):
        from r2f.modules.s1.oracles import evaluate_primary
        from r2f.campaign.triage import attribute
        facts = recovery_primary_facts()
        facts['received'].pop(1)
        oracle = evaluate_primary(facts, THIRD)
        cell = {'arm': 'mutated', 'control_kind': None, 'controls_support_sut': True,
                'provenance_supports_sut': True, 'execution_stage': 'judged',
                'expected_effect_set': S1Module().targets[THIRD].expected_effect_set,
                'mutation_receipt': dict.fromkeys(('planned', 'submitted', 'carrier_admitted', 'configured',
                                                  'applied', 'hit', 'effect_observed'), True)}
        self.assertEqual(attribute(cell, facts, oracle)['responsibility_layer'], 'sut')
        cell.update(arm='negative', control_kind='fault')
        cell['mutation_receipt'] = dict.fromkeys(cell['mutation_receipt'], False)
        self.assertNotEqual(attribute(cell, facts, oracle)['responsibility_layer'], 'sut')


def activity_fixture():
    import hashlib
    from r2f.campaign.recovery_adapter import _activity
    tokens = {role: {'endpoint_gid': prefix + '00000603'} for role, prefix in
              (('writer', '1' * 24), ('reader', '2' * 24))}
    data = b'offline-pcap-prefix-for-unit-test'
    ready = {'status': 'ready'}
    capture = {'ready': ready, 'data': data, 'stopped': {'raw': {'path': '/offline/capture.pcap'}}, 'packets': {}}
    witnesses = []
    bindings = []
    for number, (role, token) in enumerate(tokens.items()):
        prefix = token['endpoint_gid'][:24]
        body = {'source_guid_prefix': prefix, 'source_port': 36000 + number, 'destination_port': 27410,
                'kind': 'SEDP_HEARTBEAT', 'builtin_writer_id': '000003c2'}
        ref = {'packet_index': number, 'frame_offset': 0, 'captured_length': 1}
        entry = {'unix_ns': 160 + number, 'received_monotonic_ns': 110 + number,
                 'clock': 'kernel_timestampns', 'raw_ref': ref}
        capture['packets'][number] = (entry, [body])
        witnesses.append({**body, **entry})
        witnesses[-1].pop('packet_index', None)
        bindings.append({'port': 36000 + number, 'roles': [role]})
    event = {'activity': {'status': 'observed', 'since_ns': 150, 'until_ns': 170,
                          'clock': 'unix_ns_realtime', 'participant_prefixes': sorted(t['endpoint_gid'][:24] for t in tokens.values()),
                          'ready': ready, 'window_buffer_retained': True, 'health': {'status': 'ok'},
                          'raw_prefix': {'path': '/offline/capture.pcap', 'size': len(data),
                                         'sha256': hashlib.sha256(data).hexdigest()}, 'events': witnesses}}
    return event, capture, tokens, bindings


class IndependentActivityAudit(unittest.TestCase):
    def test_ephemeral_source_port_is_bound_to_correct_process_and_guid(self):
        from r2f.campaign.recovery_adapter import _activity
        event, capture, tokens, bindings = activity_fixture()
        proof = _activity(event, capture, tokens, 150, 180, {27410}, bindings, after_ns=100)
        self.assertEqual(proof['received_monotonic_ns'], 110)
        bindings[0]['roles'] = ['reader']
        with self.assertRaisesRegex(ValueError, 'not bound to pair packet'):
            _activity(event, capture, tokens, 150, 180, {27410}, bindings, after_ns=100)

    def test_both_participants_must_be_independently_observed(self):
        from r2f.campaign.recovery_adapter import _activity
        event, capture, tokens, bindings = activity_fixture()
        event['activity']['events'].pop()
        with self.assertRaisesRegex(ValueError, 'both matched participants'):
            _activity(event, capture, tokens, 150, 180, {27410}, bindings, after_ns=100)

    def test_stale_monotonic_sample_not_clamped_after_boundary(self):
        from r2f.campaign.recovery_adapter import _activity
        for stamp in (99, 100):
            event, capture, tokens, bindings = activity_fixture()
            event['activity']['events'][0]['received_monotonic_ns'] = stamp
            capture['packets'][0][0]['received_monotonic_ns'] = stamp
            with self.assertRaisesRegex(ValueError, 'not bound to pair packet'):
                _activity(event, capture, tokens, 150, 180, {27410}, bindings, after_ns=100)

    def test_prefix_hash_clock_and_raw_frame_tamper_are_rejected(self):
        from r2f.campaign.recovery_adapter import _activity
        for mode in ('hash', 'clock', 'frame', 'time'):
            event, capture, tokens, bindings = activity_fixture()
            if mode == 'hash': event['activity']['raw_prefix']['sha256'] = '0' * 64
            if mode == 'clock': event['activity']['events'][0]['clock'] = 'fallback'
            if mode == 'frame': event['activity']['events'][0]['kind'] = 'SPDP_DATA'
            if mode == 'time': event['activity']['events'][0]['unix_ns'] = 149
            with self.subTest(mode=mode), self.assertRaises(ValueError):
                _activity(event, capture, tokens, 150, 180, {27410}, bindings, after_ns=100)


def postrestore_fault_events():
    fault_id = 'post_restore_delivery_omission'
    events = [
        {'event': 'delivery_contract', 'expected_sequences': [2, 3, 4, 5], 'sentinel_sequence': 5,
         'fault_id': fault_id, 'phase': 'post_restore'},
        {'event': 'window_open', 'window': 'post_restore_delivery'},
        {'event': 'harness_omission', 'send_sequence': 4, 'phase': 'post_restore',
         'fault_id': fault_id, 'responsibility_layer': 'harness', 'candidate_eligible': False},
        {'event': 'receive', 'send_sequence': 5, 'phase': 'post_restore', 'role': 'reader'},
        {'event': 'window_close', 'window': 'post_restore_delivery'},
        {'event': 'fault_control_receipt', 'fault_id': fault_id, 'omitted_sequence': 4,
         'applied': True, 'omission_event_ref': 'offline-fault#2',
         'responsibility_layer': 'harness', 'candidate_eligible': False},
    ]
    for index, event in enumerate(events):
        event.update(event_index=index, raw_event_ref=f'offline-fault#{index}', monotonic_ns=10 + index)
    config = {'arm': 'negative', 'control_kind': 'fault', 'fault_id': fault_id}
    return events, config


class PostrestoreFaultAudit(unittest.TestCase):
    def test_real_omission_and_delivered_sentinel_map_to_core_v1_window_refs(self):
        from r2f.campaign.recovery_adapter import recovery_fault_receipt
        events, config = postrestore_fault_events()
        receipt = recovery_fault_receipt(events, config)
        self.assertEqual(receipt['schema'], 'r2f.fault-control-receipt/v1')
        self.assertEqual(receipt['receipt_kind'], 'delivery_omission')
        self.assertEqual(receipt['window'], 'post_restore_delivery')
        self.assertEqual(receipt['window_refs'], {'open_event_ref': 1, 'close_event_ref': 4})
        self.assertEqual(receipt['raw_event_ref'], 5)
        self.assertEqual(receipt['omitted_sequence'], 4)
        self.assertEqual(receipt['responsibility_layer'], 'harness')
        self.assertFalse(receipt['candidate_eligible'])

    def test_tail_without_sentinel_cannot_prove_fault_primary_failure(self):
        from r2f.campaign.recovery_adapter import recovery_fault_receipt
        events, config = postrestore_fault_events()
        events.remove(events[3])
        with self.assertRaisesRegex(ValueError, 'not independently closed'):
            recovery_fault_receipt(events, config)

    def test_closure_outside_window_and_forged_raw_reference_are_rejected(self):
        from r2f.campaign.recovery_adapter import recovery_fault_receipt
        for mode in ('early', 'late', 'wrong_ref', 'wrong_sequence', 'eligible', 'not_negative'):
            events, config = postrestore_fault_events()
            if mode == 'early': events[3]['monotonic_ns'] = events[1]['monotonic_ns'] - 1
            elif mode == 'late': events[3]['monotonic_ns'] = events[4]['monotonic_ns'] + 1
            elif mode == 'wrong_ref': events[5]['omission_event_ref'] = 'offline-fault#3'
            elif mode == 'wrong_sequence': events[5]['omitted_sequence'] = 5
            elif mode == 'eligible': events[5]['candidate_eligible'] = True
            else: config['arm'] = 'mutated'
            with self.subTest(mode=mode), self.assertRaises(ValueError):
                recovery_fault_receipt(events, config)
