"""Offline trajectory timing and raw summary tests; no ROS, namespaces or sockets."""
import ast
import copy
import inspect
import unittest
from types import SimpleNamespace
from unittest.mock import patch

from r2f.runtime.namespace_pair import _RecoveryMatchTrajectory, _body


class Clock:
    def __init__(self, now=10_000_000_000):
        self.now = now

    def monotonic_ns(self):
        return self.now

    def monotonic(self):
        return self.now / 10**9


class Fixture:
    def __init__(self):
        self.clock = Clock()
        self.events, self.commands, self.pending = [], [], []
        self.endpoints = {role: dict(pid=100 + i, process_start_ticks=10 + i,
            endpoint_generation=role + '-generation', rmw_identifier='offline-rmw',
            run_id='offline', domain_id=7, endpoint_gid=role + '-gid',
            created_monotonic_ns=8_000_000_000) for i, role in enumerate(('writer', 'reader'))}
        self.serial = self.event_serial = 0
        self.counts = {'writer': 1, 'reader': 1}
        self.empty_remote = set()
        self.omit = set()
        self.request_record_delay_ns = 0
        before = self.gate(9_000_000_000)
        self.trajectory = _RecoveryMatchTrajectory(self.events, self.endpoints, self.emit,
            10_000_000_000, 13_000_000_000, before)

    def emit(self, event, **fields):
        index = self.event_serial
        self.event_serial += 1
        token = dict(event=event, monotonic_ns=self.clock.now,
                     namespace_event_index=index, raw_event_ref='offline#' + str(index))
        token.update(fields)
        self.events.append(token)
        return token

    def responses(self, role, cid, requested, count=1, empty_remote=False):
        identity = {k: self.endpoints[role][k] for k in _RecoveryMatchTrajectory.IDENTITY_KEYS}
        common = dict(identity, role=role, command_id=cid)
        snapshot = self.emit('match_snapshot', **common, matched_count=count,
            endpoint_gid=self.endpoints[role]['endpoint_gid'], sampled_monotonic_ns=requested + 1_000_000,
            count_api='get_subscription_count' if role == 'writer' else 'get_publisher_count',
            monotonic_ns=requested + 2_000_000)
        graph = self.emit('graph_snapshot', **common, monotonic_ns=requested + 3_000_000,
            publishers=[] if empty_remote and role == 'reader' else [{'endpoint_gid': 'writer-gid'}],
            subscriptions=[] if empty_remote and role == 'writer' else [{'endpoint_gid': 'reader-gid'}])
        ack = self.emit('command_done', **common, monotonic_ns=requested + 4_000_000)
        return snapshot, graph, ack

    def gate(self, requested):
        evidence = {'matches': {}, 'graphs': {}}
        for role in ('writer', 'reader'):
            self.serial += 1
            self.emit('match_snapshot_requested', role=role, command_id=self.serial,
                      requested_monotonic_ns=requested, monotonic_ns=requested)
            snapshot, graph, _ = self.responses(role, self.serial, requested)
            evidence['matches'][role] = snapshot['raw_event_ref']
            evidence['graphs'][role] = graph['raw_event_ref']
        return evidence

    def command(self, role, command, **payload):
        assert command == 'snapshot'
        self.serial += 1
        self.commands.append((role, self.serial, payload))
        self.pending.append((role, self.serial, payload['requested_monotonic_ns'],
                             self.counts[role], role in self.empty_remote))
        self.clock.now += self.request_record_delay_ns
        return self.serial

    def flush(self):
        pending, self.pending = self.pending, []
        for role, cid, requested, count, empty in pending:
            responses = self.responses(role, cid, requested, count, empty)
            for response in responses:
                if (role, response['event']) in self.omit:
                    self.events.remove(response)

    def sample(self):
        self.clock.now = self.trajectory.next_due_ns
        with patch('r2f.runtime.namespace_pair.time', self.clock):
            self.trajectory.poll(self)
        self.flush()

    def finish(self):
        while len(self.trajectory.samples) < len(self.trajectory.schedule):
            self.sample()
        self.trajectory.restore_started_ns = 13_000_000_000
        self.trajectory.restore_boundary_ns = 13_100_000_000
        self.trajectory.after = self.gate(13_200_000_000)
        self.emit('window_open', window='post_restore_delivery', monotonic_ns=13_300_000_000)
        return self.trajectory.summary()


class MatchTrajectory(unittest.TestCase):
    def test_all_scheduled_samples_complete_means_only_no_loss_at_samples(self):
        f = Fixture()
        result = f.finish()
        self.assertEqual(result['status'], 'no_loss_at_sampled_points')
        self.assertEqual([s['scheduled_monotonic_ns'] for s in result['samples']],
                         [11_000_000_000, 12_000_000_000, 12_500_000_000])
        self.assertEqual(result['samples'][-1]['purpose'], 'pre_restore')
        self.assertFalse(result['primary_verdict_input'])
        self.assertIn('lease expiry', result['limitation'])
        self.assertEqual(len(f.commands), 6)
        self.assertEqual(len({cid for _, cid, _ in f.commands}), 6)
        self.assertTrue(all(p['diagnostic_purpose'] == 'discovery_match_trajectory' for _, _, p in f.commands))

    def test_bilateral_count_zero_and_remote_absence_with_pre_post_proves_sampled_loss(self):
        f = Fixture()
        f.counts = dict(writer=0, reader=0)
        f.empty_remote = {'writer', 'reader'}
        f.sample()
        f.counts = dict(writer=1, reader=1)
        f.empty_remote.clear()
        result = f.finish()
        self.assertEqual(result['status'], 'observed_loss_then_recovery')
        self.assertEqual(result['bilateral_loss_sample_indices'], [0])
        for role in ('writer', 'reader'):
            self.assertEqual(result['samples'][0]['roles'][role]['matched_count'], 0)
            self.assertFalse(result['samples'][0]['roles'][role]['remote_guid_present'])
        self.assertTrue(result['before']['both_matched'])
        self.assertTrue(result['after']['both_matched'])
        self.assertEqual(sum(e['event'] == 'match_snapshot' and e.get('matched_count') == 0 for e in f.events), 2)

    def test_single_role_loss_and_count_graph_disagreement_stay_partial(self):
        for remote_missing in (False, True):
            with self.subTest(remote_missing=remote_missing):
                f = Fixture()
                f.counts['writer'] = 0
                if remote_missing:
                    f.empty_remote.add('writer')
                result = f.finish()
                self.assertEqual(result['status'], 'incomplete')
                self.assertEqual(result['bilateral_loss_sample_indices'], [])
                self.assertEqual(result['samples'][0]['roles']['writer']['matched_count'], 0)
                self.assertEqual(result['samples'][0]['loss_roles'], ['writer'] if remote_missing else [])

    def test_loss_at_different_rounds_cannot_be_combined_into_bilateral_loss(self):
        f = Fixture()
        f.counts['writer'] = 0
        f.empty_remote = {'writer'}
        f.sample()
        f.counts = dict(writer=1, reader=0)
        f.empty_remote = {'reader'}
        result = f.finish()
        self.assertEqual(result['status'], 'incomplete')
        self.assertFalse(result['bilateral_loss_sample_indices'])

    def test_parent_request_record_may_follow_worker_sample_and_ack(self):
        f = Fixture()
        f.request_record_delay_ns = 10_000_000
        result = f.finish()
        self.assertEqual(result['status'], 'no_loss_at_sampled_points')
        request = result['samples'][0]['requests']['writer']
        sample = result['samples'][0]['roles']['writer']
        self.assertGreater(request['monotonic_ns'], sample['sampled_monotonic_ns'])
        self.assertEqual(request['requested_monotonic_ns'], f.commands[0][2]['requested_monotonic_ns'])

    def test_missing_events_do_not_become_no_loss(self):
        for kind in ('match_snapshot', 'graph_snapshot', 'command_done'):
            with self.subTest(kind=kind):
                f = Fixture()
                f.omit.add(('writer', kind))
                result = f.finish()
                self.assertEqual(result['status'], 'incomplete')
                self.assertIn(kind + '_missing_or_duplicate', result['samples'][0]['roles']['writer']['errors'])

    def test_missing_ack_keeps_zero_count_and_empty_remote_graph_in_summary(self):
        f = Fixture()
        f.counts = dict(writer=0, reader=0)
        f.empty_remote = {'writer', 'reader'}
        f.omit.add(('writer', 'command_done'))
        result = f.finish()
        writer = result['samples'][0]['roles']['writer']
        self.assertEqual(writer['matched_count'], 0)
        self.assertEqual(writer['subscription_gids'], [])
        self.assertTrue(writer['timed_out'])
        self.assertEqual(writer['command_done_refs'], [])
        self.assertEqual(result['status'], 'incomplete')

    def test_late_ack_or_sample_at_deadline_remains_referenced_but_incomplete(self):
        for kind, key in (('match_snapshot', 'sampled_monotonic_ns'), ('command_done', 'monotonic_ns')):
            with self.subTest(kind=kind):
                f = Fixture()
                f.sample()
                request = f.trajectory.samples[0]['requests']['writer']
                event = next(e for e in f.events if e['event'] == kind and e.get('command_id') == request['command_id'])
                event[key] = request['deadline_monotonic_ns']
                result = f.finish()
                self.assertEqual(result['status'], 'incomplete')
                self.assertIn(event['raw_event_ref'], result['samples'][0]['roles']['writer'][kind + '_refs'])

    def test_post_restore_sample_cannot_be_used_as_drop_sample(self):
        f = Fixture()
        f.sample()
        cid = f.trajectory.samples[0]['requests']['writer']['command_id']
        for e in f.events:
            if e.get('command_id') == cid and e['event'] in ('match_snapshot', 'graph_snapshot', 'command_done'):
                e['monotonic_ns'] = 14_000_000_000
                if e['event'] == 'match_snapshot':
                    e['sampled_monotonic_ns'] = 14_000_000_000
        self.assertEqual(f.finish()['status'], 'incomplete')

    def test_wrong_identity_command_duplicates_and_clock_order_are_incomplete(self):
        cases = [('pid', -1), ('process_start_ticks', -1), ('endpoint_generation', 'wrong'),
                 ('rmw_identifier', 'wrong'), ('role', 'reader'), ('command_id', -1),
                 ('monotonic_ns', 1), ('sampled_monotonic_ns', 1), ('duplicate', True)]
        for field, value in cases:
            with self.subTest(field=field):
                f = Fixture()
                f.sample()
                cid = f.trajectory.samples[0]['requests']['writer']['command_id']
                snapshot = next(e for e in f.events if e['event'] == 'match_snapshot' and e.get('command_id') == cid)
                if field == 'duplicate':
                    f.events.append(copy.deepcopy(snapshot))
                else:
                    snapshot[field] = value
                self.assertEqual(f.finish()['status'], 'incomplete')

    def test_wrong_ack_pid_cannot_complete_trajectory(self):
        f = Fixture()
        f.sample()
        cid = f.trajectory.samples[0]['requests']['writer']['command_id']
        next(e for e in f.events if e['event'] == 'command_done' and e.get('command_id') == cid)['pid'] = -1
        self.assertEqual(f.finish()['status'], 'incomplete')

    def test_missing_pre_or_post_cannot_claim_recovery(self):
        for which in ('before', 'after'):
            with self.subTest(which=which):
                f = Fixture()
                f.counts = dict(writer=0, reader=0)
                f.empty_remote = {'writer', 'reader'}
                f.finish()
                setattr(f.trajectory, which, None)
                self.assertEqual(f.trajectory.summary()['status'], 'incomplete')

    def test_unattempted_and_expired_samples_are_incomplete_without_command_retry(self):
        f = Fixture()
        f.clock.now = f.trajectory.deadline_ns
        with patch('r2f.runtime.namespace_pair.time', f.clock):
            f.trajectory.poll(f)
        self.assertEqual(f.commands, [])
        self.assertTrue(all(s['errors'] for s in f.trajectory.samples))
        self.assertEqual(f.finish()['status'], 'incomplete')
        f = Fixture()
        result = f.trajectory.summary()
        self.assertEqual(result['status'], 'incomplete')
        self.assertEqual(len(result['unsampled_scheduled_monotonic_ns']), 3)

    def test_command_failure_is_preserved_as_diagnostic_gap(self):
        f = Fixture()
        def broken(*args, **kwargs):
            raise BrokenPipeError('offline injected failure')
        f.command = broken
        result = f.finish()
        self.assertEqual(result['status'], 'incomplete')
        self.assertIn('BrokenPipeError', result['samples'][0]['errors'][0])

    def run_actual_interruption_loop(self, fail_counter=False):
        # Execute the actual drop loop/finally AST with a virtual clock and pool.
        # No _body admission, source manifest, sockets, subprocesses or ROS run.
        node = ast.parse(inspect.getsource(_body))
        loop = next(n for n in ast.walk(node) if isinstance(n, ast.Try) and n.body
                    and isinstance(n.body[0], ast.While) and n.finalbody
                    and any(isinstance(x, ast.Assign) and any(isinstance(t, ast.Attribute)
                            and t.attr == 'restore_started_ns' for t in x.targets) for x in n.finalbody))
        f = Fixture()
        restored = []
        def counters():
            if fail_counter:
                raise RuntimeError('offline counter failure')
            return {'hit': True}
        carrier = SimpleNamespace(counters=counters, restore=lambda: restored.append(f.clock.now) or {'restored': True})
        def pause(seconds):
            self.assertGreaterEqual(seconds, 0)
            f.clock.now += max(1, round(seconds * 10**9))
            f.flush()
        context = dict(time=f.clock, c={'counter_interval_sec': .25}, active=True, carrier=carrier,
            trajectory=f.trajectory, pool=f, emit=f.emit, counter_hit=False, deadline=13., pause=pause,
            activity=lambda *a: {'activity': {}}, drop_wall_ns=0, activity_is_pair_observed=lambda a: True)
        module = ast.fix_missing_locations(ast.Module(body=[loop], type_ignores=[]))
        with patch('r2f.runtime.namespace_pair.time', f.clock):
            if fail_counter:
                with self.assertRaisesRegex(RuntimeError, 'offline counter failure'):
                    exec(compile(module, '<actual-interruption-loop>', 'exec'), context)
            else:
                f.omit.add(('writer', 'command_done'))
                exec(compile(module, '<actual-interruption-loop>', 'exec'), context)
        return f, restored

    def test_missing_ack_never_extends_drop_or_prevents_restore(self):
        f, restored = self.run_actual_interruption_loop()
        self.assertEqual(restored, [13_000_000_000])
        self.assertEqual(f.clock.now, f.trajectory.deadline_ns)
        self.assertEqual(len(f.commands), 6)

    def test_counter_failure_still_executes_original_restore_finally(self):
        f, restored = self.run_actual_interruption_loop(fail_counter=True)
        self.assertEqual(restored, [10_000_000_000])
        self.assertEqual(f.commands, [])


if __name__ == '__main__':
    unittest.main()
