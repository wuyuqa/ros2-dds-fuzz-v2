"""Offline synthetic recovery evidence; never a real SUT/ROS execution claim.

Reuse existing namespace and decoded-packet fixtures. Only _capture (the pcap
acquisition/decoder boundary) is substituted. Count/graph/ACK admission, activity
validation, recovery_facts and the primary oracle run unmodified.
"""
import copy
import hashlib
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from test_campaign_recovery import THIRD, activity_fixture, recovery_preconditions
from r2f.campaign import recovery_adapter
from r2f.campaign.runtime_adapter import fresh_match_proof
from r2f.modules.s1.oracles import evaluate_primary
from r2f.runtime.discovery_profiles import CYCLONE_XML


class OfflineRecoveryTrajectory:
    """Private synthetic raw events, carrier receipts and actual profile bytes."""

    def __init__(self, root):
        _, self.runtime, seed = recovery_preconditions()
        self.events = seed[:-2]  # Replace only this private fixture's begin/snapshot.
        self.isolation = seed[2]['isolation']
        self.config = dict(run_id='offline-recovery', domain_id=80, arm='mutated',
                           rmw='rmw_cyclonedds_cpp', topic='/offline/trajectory',
                           message_type='std_msgs/msg/String', qos={'reliability': 'reliable'},
                           reader_delay_sec=0.1, drop_duration_sec=0.1,
                           pre_match_count=1, post_match_count=3, fault_id=None)
        self.activity_template, self.capture, self.tokens, sources = activity_fixture()
        self.capture['packets'] = {}
        self.command_id = 0
        for number, (role, token) in enumerate(self.tokens.items()):
            token.update(role=role, pid=200 + number, endpoint_generation=number + 1,
                         rmw_identifier=self.config['rmw'], topic=self.config['topic'],
                         message_type=self.config['message_type'], qos=self.config['qos'])
        writer = self.emit('endpoint_created', **self.tokens['writer'])
        writer['created_monotonic_ns'] = writer['monotonic_ns']
        self.tokens['writer'] = writer
        prejoin = self.emit('send', role='writer', phase='prejoin', send_sequence=0,
                            writer_gid=writer['endpoint_gid'], publish_returned=True)
        self.emit('prejoin_publish_complete', count=1, publish_refs=[prejoin['raw_event_ref']],
                  post_restore_delivery_obligation=False)
        delay = self.emit('reader_creation_delay_begin', configured_delay_sec=0.1)
        self.emit('reader_creation_delay_end', configured_delay_sec=0.1,
                  delay_begin_ref=delay['raw_event_ref'])
        reader = self.emit('endpoint_created', **self.tokens['reader'])
        reader['created_monotonic_ns'] = reader['monotonic_ns']
        self.tokens['reader'] = reader
        self.initial = self.query_pair(1)
        base = 7400 + 250 * self.config['domain_id']
        self.ports = {base, *(base + 10 + 2 * p for p in range(32))}
        self.bindings = dict(only_pair_bound_udp_sockets=True,
                             worker_pids={r: t['pid'] for r, t in self.tokens.items()},
                             isolation=self.isolation, worker_start_ticks={'writer': 41, 'reader': 42},
                             bindings=[dict(b, selected_discovery_port=False) for b in sources] +
                             [dict(port=base + 10 + 2 * n, roles=[r], selected_discovery_port=True)
                              for n, r in enumerate(self.tokens)])
        snapshot = self.emit('discovery_precondition_snapshot', bindings=self.bindings,
                             worker_pids=self.bindings['worker_pids'],
                             matched={kind: {r: round_[kind]['raw_event_ref']
                                             for r, round_ in self.initial.items()}
                                      for kind in ('matches', 'graphs')},
                             **{r + '_gid': t['endpoint_gid'] for r, t in self.tokens.items()})
        self.begin = self.emit('mutation_begin')
        table = 'r2f_discovery_0123456789abcdef'
        self.emit('discovery_drop_begin', receipt=dict(
            scope='ros2_pair', ros2_execution=True, configured=True, applied=True,
            table=table, pair_bindings=self.bindings,
            precondition={'evidence_ref': snapshot['raw_event_ref'], 'match_established': True},
            install_command=self.command(['nft', '-f', '-'])))
        self.interruption = self.emit('window_open', window='discovery_interruption')
        self.intermediate = self.query_pair(0)
        self.activity('discovery_interruption', self.interruption['time_ns'])
        nft = {'nftables': [
            {'counter': dict(family='inet', table=table, name='discovery_drop', packets=12, bytes=120)},
            {'rule': dict(table=table, expr=[
                {'match': {'left': {'meta': {'key': 'iifname'}}, 'right': 'lo'}},
                {'match': {'left': {'payload': {'protocol': 'udp', 'field': 'dport'}},
                           'op': '==', 'right': {'set': sorted(self.ports)}}}, {'drop': None}])}]}
        counter = dict(table=table, counter_observed=True, packets=12, bytes=120, hit=True,
                       nft_json=nft, command=self.command(['nft', '-j', 'list', 'table', 'inet', table], nft))
        restore = self.emit('discovery_drop_end', receipt=dict(
            table=table, restored=True, table_absent=True, counter_snapshot=counter,
            delete_command=self.command(['nft', 'delete', 'table', 'inet', table]),
            verify_command=self.command(['nft', '-j', 'list', 'ruleset'], {'nftables': []}),
            final_nft={'nftables': []}))
        restore['receipt']['drop_end_monotonic_ns'] = restore['monotonic_ns']
        self.emit('window_close', window='discovery_interruption')
        self.boundary = self.emit('discovery_restore_boundary', restored=True, neutral=False,
                                  clock_guard_ns=50_000_000)
        self.boundary.update(post_restore_monotonic_ns=self.boundary['monotonic_ns'],
                             post_restore_unix_ns=self.boundary['time_ns'],
                             activity_since_unix_ns=self.boundary['time_ns'] + 50_000_000)
        observed = self.activity('post_restore', self.boundary['activity_since_unix_ns'])
        observed.update(after_monotonic_ns=self.boundary['monotonic_ns'], clock_guard_ns=50_000_000)
        self.emit('discovery_recovery_confirmed', receipt=dict(
            recovery_verified=True, restore_end_monotonic_ns=restore['monotonic_ns'],
            activity_receipt={'evidence_ref': observed['raw_event_ref']}, pair_bindings=self.bindings))
        self.final = self.query_pair(1)
        self.observe('observe_recovery_graph')
        expected = [1, 2, 3]
        self.emit('delivery_contract', expected_sequences=expected, fault_id=None, sentinel_sequence=3,
                  phase='post_restore', qos=self.config['qos'], prejoin_in_delivery_contract=False)
        self.opening = self.emit('window_open', window='post_restore_delivery', expected_sequences=expected)
        for sequence in expected:
            self.emit('send', role='writer', phase='post_restore', send_sequence=sequence,
                      writer_gid=writer['endpoint_gid'], publish_returned=True)
            self.emit('receive', role='reader', phase='post_restore', send_sequence=sequence,
                      reader_gid=reader['endpoint_gid'], publisher_gid=writer['endpoint_gid'],
                      payload_run_id=self.config['run_id'])
        self.emit('window_close', window='post_restore_delivery')
        for role in self.tokens:
            self.emit('graph_snapshot', role=role, **self.graph(1, role))
        self.observe('observe_final_graph')
        self.emit('discovery_cleanup_restore', receipt={'restored': True})
        self.emit('namespace_controller_exited', returncode=0)
        root = Path(root)
        profile = root / 'cyclone.xml'
        profile.write_text(CYCLONE_XML)
        record = {'path': str(profile), 'sha256': hashlib.sha256(profile.read_bytes()).hexdigest()}
        self.runtime.update(raw_path=str(root / 'raw.jsonl'), environment={
            'ok': True, 'discovery_namespace': {'profiles': {r: record for r in self.tokens}}},
            carrier_implementation=recovery_adapter.IMPLEMENTATION,
            execution_metadata={'workers': [dict(role=r, pid=t['pid'], returncode=0)
                                            for r, t in self.tokens.items()]})

    def emit(self, name, role=None, **fields):
        index = len(self.events)
        mono = (index + 1) * 100_000_000
        identity = {k: self.tokens[role][k] for k in ('pid', 'endpoint_generation', 'rmw_identifier')} if role else {}
        event = {**fields, **identity, 'event': name, 'event_index': index,
                 'raw_event_ref': f'offline#{index}', 'monotonic_ns': mono,
                 'time_ns': 1_000_000_000_000 + mono}
        if role:
            event['role'] = role
        self.events.append(event)
        return event

    def graph(self, count, role):
        # A zero public count retains the local endpoint and has an empty remote graph.
        return {'publishers': ([{'endpoint_gid': self.tokens['writer']['endpoint_gid']}]
                               if count or role == 'writer' else []),
                'subscriptions': ([{'endpoint_gid': self.tokens['reader']['endpoint_gid']}]
                                  if count or role == 'reader' else [])}

    def query_pair(self, count):
        result = {}
        for role in self.tokens:
            self.command_id += 1
            request = self.emit('match_snapshot_requested', role=role, command_id=self.command_id,
                                reader_created_monotonic_ns=self.tokens['reader']['created_monotonic_ns'])
            request['requested_monotonic_ns'] = request['monotonic_ns'] - 1
            match = self.emit('match_snapshot', role=role, command_id=self.command_id,
                              endpoint_gid=self.tokens[role]['endpoint_gid'], matched_count=count,
                              count_api='get_subscription_count' if role == 'writer' else 'get_publisher_count')
            match['sampled_monotonic_ns'] = match['monotonic_ns'] - 1
            graph = self.emit('graph_snapshot', role=role, command_id=self.command_id, **self.graph(count, role))
            ack = self.emit('command_done', role=role, command_id=self.command_id)
            result[role] = dict(request=request, matches=match, graphs=graph, ack=ack)
        return result

    def command(self, argv, payload=None):
        return dict(argv=argv, returncode=0, net_namespace=self.isolation['net_namespace'],
                    started_monotonic_ns=1, ended_monotonic_ns=2, stdout=json.dumps(payload))

    def activity(self, window, since):
        event = self.emit('discovery_activity_observation', window=window,
                          activity=copy.deepcopy(self.activity_template['activity']))
        activity = event['activity']
        activity.update(since_ns=since, until_ns=event['time_ns'])
        packet_base = len(self.capture['packets'])
        for number, witness in enumerate(activity['events']):
            packet = packet_base + number
            witness['raw_ref']['packet_index'] = packet
            witness['unix_ns'] = event['time_ns'] - 2 + number
            witness['received_monotonic_ns'] = event['monotonic_ns'] - 2 + number
            entry = {k: copy.deepcopy(witness[k]) for k in ('raw_ref', 'unix_ns', 'clock', 'received_monotonic_ns')}
            body = {k: copy.deepcopy(v) for k, v in witness.items()
                    if k not in ('raw_ref', 'unix_ns', 'clock', 'received_monotonic_ns')}
            self.capture['packets'][packet] = (entry, [body])
        return event

    def observe(self, step_id):
        actual = dict(step_id=step_id, satisfied=True, before={'monotonic_ns': self.events[-1]['monotonic_ns']},
                      after=None, evidence={'offline_step': step_id})
        before = self.emit('state_step_before', **copy.deepcopy(actual))
        actual['before']['raw_event_ref'] = before['raw_event_ref']
        actual['after'] = {'monotonic_ns': before['monotonic_ns'] + 1}
        after = self.emit('state_step_after', **copy.deepcopy(actual))
        actual['after']['raw_event_ref'] = after['raw_event_ref']
        self.runtime.setdefault('observation_receipts', []).append(actual)

    def facts(self):
        # Synthetic decoded packets only; no capture process, socket or ROS runtime.
        with patch.object(recovery_adapter, '_capture', return_value=self.capture) as capture:
            result = recovery_adapter.recovery_facts(self.events, self.runtime, self.config)
        if capture.call_count != 1:
            raise AssertionError('fixture failed before the intended capture/admission boundary')
        return result


class RecoveryTrajectoryAdmission(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.fixture = OfflineRecoveryTrajectory(self.temp.name)

    def assert_unknown_evidence(self, expected):
        before = copy.deepcopy(self.fixture.events)
        facts = self.fixture.facts()
        self.assertRegex(facts['evidence_error'], expected)
        self.assertFalse(facts['endpoint_identity_complete'])
        self.assertFalse(facts['window']['complete'])
        primary = evaluate_primary(facts, THIRD)
        self.assertEqual(primary['verdict'], 'unknown')
        self.assertEqual(primary['forbidden_residuals'], [])
        self.assertTrue(primary['reason_codes'][0].startswith('unknown_recovery_evidence:'))
        self.assertEqual(self.fixture.events, before, 'admission must preserve failed raw evidence')

    def test_intermediate_zero_counts_empty_remote_graphs_do_not_poison_recovery(self):
        fixture = self.fixture
        before = copy.deepcopy(fixture.events)
        facts = fixture.facts()
        self.assertNotIn('evidence_error', facts)
        self.assertTrue(facts['endpoint_identity_complete'])
        self.assertEqual(facts['recovery']['match_continuity'], 'reestablished')
        self.assertEqual(facts['recovery']['match_continuity_scope'], 'observed_public_count_samples')
        self.assertEqual(evaluate_primary(facts, THIRD)['verdict'], 'pass')
        commands = [round_['request']['command_id'] for pair in
                    (fixture.initial, fixture.intermediate, fixture.final) for round_ in pair.values()]
        self.assertEqual(len(set(commands)), 6)
        for role, round_ in fixture.intermediate.items():
            remote = 'subscriptions' if role == 'writer' else 'publishers'
            self.assertEqual(round_['matches']['matched_count'], 0)
            self.assertEqual(round_['graphs'][remote], [])
            self.assertLess(fixture.interruption['event_index'], round_['request']['event_index'])
            self.assertLess(round_['request']['event_index'], round_['matches']['event_index'])
            self.assertLess(round_['matches']['event_index'], round_['graphs']['event_index'])
            self.assertLess(round_['graphs']['event_index'], round_['ack']['event_index'])
            self.assertEqual(facts['match_snapshot_proof'][role]['command_id'],
                             fixture.final[role]['request']['command_id'])
        self.assertEqual(fixture.events, before)

    def test_old_recovery_pair_cannot_close_restored_window(self):
        for round_ in self.fixture.final.values():
            round_['request']['event'] = 'offline_removed_request'
        self.assert_unknown_evidence('request pair incomplete')

    def test_single_role_recovery_cannot_borrow_the_other_old_role(self):
        self.fixture.final['reader']['request']['event'] = 'offline_removed_request'
        self.assert_unknown_evidence('request pair incomplete')

    def test_requests_before_restore_boundary_cannot_close_recovery(self):
        for round_ in self.fixture.final.values():
            round_['request']['requested_monotonic_ns'] = self.fixture.boundary['monotonic_ns'] - 1
        self.assert_unknown_evidence('request pair incomplete')

    def test_reused_command_cannot_relabel_old_match_graph_ack(self):
        # Reuse a drop-period command: initial pre-drop proof stays valid.
        old_id = self.fixture.intermediate['writer']['request']['command_id']
        for event in self.fixture.final['writer'].values():
            event['command_id'] = old_id
        self.assert_unknown_evidence('command binding.*observed 2')

    def test_zero_recovery_count_is_unknown_evidence_not_primary_failure(self):
        self.fixture.final['reader']['matches']['matched_count'] = 0
        self.assert_unknown_evidence('count/API/time/command proof invalid')

    def test_empty_recovery_graph_is_unknown_evidence_not_primary_failure(self):
        self.fixture.final['reader']['graphs']['publishers'] = []
        self.assert_unknown_evidence('fresh recovery graph/count identity mismatch')

    def test_missing_recovery_ack_is_unknown_evidence_not_primary_failure(self):
        self.fixture.final['reader']['ack']['event'] = 'offline_missing_ack'
        self.assert_unknown_evidence('command binding.*observed 0')

    def test_fresh_match_proof_selects_only_post_boundary_pair(self):
        fixture = self.fixture
        matched_ns, proof = fresh_match_proof(fixture.events, fixture.tokens, fixture.opening,
                                             anchor_ns=fixture.boundary['monotonic_ns'])
        self.assertEqual(matched_ns, fixture.final['reader']['ack']['monotonic_ns'])
        for role in fixture.tokens:
            self.assertEqual(proof[role]['request_index'], fixture.final[role]['request']['event_index'])
            self.assertGreaterEqual(fixture.final[role]['request']['requested_monotonic_ns'],
                                    fixture.boundary['monotonic_ns'])


if __name__ == '__main__':
    unittest.main()
