"""Offline regressions for discovery callbacks preceding create_subscription return."""
import time
import unittest
from unittest.mock import Mock

from r2f.runtime.ros2_pair import wait_for_fresh_match
from r2f.runtime.worker import emit_snapshot


class SnapshotWorkerContract(unittest.TestCase):
    def test_snapshot_invokes_role_count_api_even_without_count_transition(self):
        for role, api in [('writer', 'get_subscription_count'), ('reader', 'get_publisher_count')]:
            with self.subTest(role=role):
                endpoint = Mock()
                getattr(endpoint, api).return_value = 1
                tokens = []
                emit = lambda event, **fields: tokens.append({'event': event, **fields})
                graph = Mock(return_value={'publishers': [], 'subscriptions': []})
                emit_snapshot(endpoint, role, 31, 'gid', graph, emit)
                emit_snapshot(endpoint, role, 32, 'gid', graph, emit)
                self.assertEqual(getattr(endpoint, api).call_count, 2)
                snapshots = [e for e in tokens if e['event'] == 'match_snapshot']
                self.assertEqual([e['command_id'] for e in snapshots], [31, 32])
                self.assertTrue(all(e['matched_count'] == 1 and e['count_api'] == api for e in snapshots))
                self.assertTrue(all(type(e['sampled_monotonic_ns']) is int for e in snapshots))
                self.assertEqual(graph.call_count, 2)


class ControlledPool:
    """Only drives helper logic; never creates ROS, workers, raw or execution metadata."""
    def __init__(self, events, anchor, *, fresh=True, initial_zero=False,
                 acknowledgements=True, delayed_ack=False, early_ack=False, duplicate_ack=False):
        self.events, self.anchor = events, anchor
        self.fresh, self.initial_zero = fresh, initial_zero
        self.serial, self.pending, self.requests = 0, [], []
        self.acknowledgements, self.delayed_ack = acknowledgements, delayed_ack
        self.early_ack, self.duplicate_ack = early_ack, duplicate_ack
        self.pump_count = 0

    def command(self, role, command):
        self.serial += 1
        self.pending.append((role, self.serial))
        self.requests.append((role, command, self.serial))
        return self.serial

    def wait(self, predicate, timeout, label):
        acks = []
        self.pump_count += 1
        for role, cid in self.pending:
            now = time.monotonic_ns()
            if self.fresh:
                self.events.append({'event': 'match_snapshot', 'role': role, 'command_id': cid,
                                    'sampled_monotonic_ns': now, 'monotonic_ns': now + 1,
                                    'matched_count': 0 if self.initial_zero and cid <= 2 else 1})
                if self.acknowledgements:
                    ack = {'event': 'command_done', 'role': role, 'command_id': cid,
                           'monotonic_ns': now if self.early_ack else now + 2}
                    acks.append(ack)
                    if self.duplicate_ack:
                        acks.append(dict(ack))
        self.pending.clear()
        if self.delayed_ack:
            # Model a read ending after match_snapshot: returning now loses the
            # later stdout chunk containing command_done.
            if predicate():
                return
            self.pump_count += 1
        self.events.extend(acks)
        if not predicate():
            raise TimeoutError(label)


class FreshMatchRace(unittest.TestCase):
    def events(self):
        anchor = time.monotonic_ns()
        return anchor, [
            {'event': 'match', 'role': 'writer', 'matched_count': 1, 'monotonic_ns': anchor - 747061},
            {'event': 'endpoint_created', 'role': 'reader', 'created_monotonic_ns': anchor},
            {'event': 'match', 'role': 'reader', 'matched_count': 1, 'monotonic_ns': anchor + 1},
        ]

    def test_pre_return_writer_match_is_not_enough(self):
        anchor, events = self.events()
        pool = ControlledPool(events, anchor, fresh=False)
        with self.assertRaises(TimeoutError):
            wait_for_fresh_match(pool, events, anchor, 1)
        self.assertEqual(len(pool.requests), 2)

    def test_fresh_counts_close_create_return_race_without_rewriting_history(self):
        anchor, events = self.events()
        old = dict(events[0])
        pool = ControlledPool(events, anchor)
        proof = wait_for_fresh_match(pool, events, anchor, 1)
        self.assertEqual(set(proof), {'writer', 'reader'})
        self.assertEqual(events[0], old)
        self.assertTrue(all(e['sampled_monotonic_ns'] >= anchor for e in proof.values()))
        self.assertEqual([e['command_id'] for e in proof.values()], [1, 2])

    def test_zero_snapshot_requires_new_commands_not_old_match_event(self):
        anchor, events = self.events()
        pool = ControlledPool(events, anchor, initial_zero=True)
        proof = wait_for_fresh_match(pool, events, anchor, 1)
        self.assertEqual(len(pool.requests), 4)
        self.assertEqual([e['command_id'] for e in proof.values()], [3, 4])

    def test_snapshot_from_another_command_is_not_accepted(self):
        anchor, events = self.events()
        for role in ('writer', 'reader'):
            events.append({'event': 'match_snapshot', 'role': role, 'command_id': 999,
                           'sampled_monotonic_ns': anchor + 1, 'monotonic_ns': anchor + 2, 'matched_count': 1})
        with self.assertRaises(TimeoutError):
            wait_for_fresh_match(ControlledPool(events, anchor, fresh=False), events, anchor, 1)

    def test_snapshot_without_ack_does_not_release_match_gate(self):
        anchor, events = self.events()
        pool = ControlledPool(events, anchor, acknowledgements=False)
        with self.assertRaises(TimeoutError):
            wait_for_fresh_match(pool, events, anchor, 1)

    def test_delayed_ack_in_next_stdout_chunk_is_consumed_before_return(self):
        anchor, events = self.events()
        pool = ControlledPool(events, anchor, delayed_ack=True)
        proof = wait_for_fresh_match(pool, events, anchor, 1)
        self.assertEqual(pool.pump_count, 2)
        for role, snapshot in proof.items():
            acks = [e for e in events if e['event'] == 'command_done' and e['role'] == role
                    and e['command_id'] == snapshot['command_id']]
            self.assertEqual(len(acks), 1)
            self.assertGreaterEqual(acks[0]['monotonic_ns'], snapshot['monotonic_ns'])

    def test_ack_before_snapshot_timestamp_is_not_evidence(self):
        anchor, events = self.events()
        with self.assertRaises(TimeoutError):
            wait_for_fresh_match(ControlledPool(events, anchor, early_ack=True), events, anchor, 1)

    def test_duplicate_ack_does_not_form_unambiguous_command_proof(self):
        anchor, events = self.events()
        with self.assertRaises(TimeoutError):
            wait_for_fresh_match(ControlledPool(events, anchor, duplicate_ack=True), events, anchor, 1)


if __name__ == '__main__':
    unittest.main()
