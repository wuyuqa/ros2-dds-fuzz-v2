"""Command-correlated graph proof, all offline; no ROS workers are started."""
import time
import unittest
from unittest.mock import Mock

from r2f.runtime.ros2_pair import query_acknowledged_graphs


class GraphPool:
    def __init__(self, events, mode='ok'):
        self.events, self.mode = events, mode
        self.pending, self.serial, self.pumps = [], 0, 0

    def command(self, role, command):
        self.serial += 1
        self.pending.append((role, self.serial))
        return self.serial

    def wait(self, predicate, timeout, label):
        self.pumps += 1
        acks = []
        for role, cid in self.pending:
            now = time.monotonic_ns()
            common = {'role': role, 'command_id': cid}
            snapshot = {'event': 'match_snapshot', **common, 'sampled_monotonic_ns': now, 'monotonic_ns': now + 1}
            graph = {'event': 'graph_snapshot', **common, 'monotonic_ns': now + 2, 'publishers': [], 'subscriptions': []}
            ack = {'event': 'command_done', **common, 'monotonic_ns': now + 3}
            self.events.append(snapshot)
            if self.mode == 'wrong_command':
                graph['command_id'] = cid + 10
            if self.mode == 'ack_first':
                self.events.append(ack)
            if self.mode != 'no_graph':
                self.events.append(graph)
            if self.mode == 'duplicate_graph':
                self.events.append(dict(graph))
            if self.mode == 'early_ack':
                ack['monotonic_ns'] = now
            if self.mode not in ('no_ack', 'ack_first'):
                acks.append(ack)
            if self.mode == 'duplicate_ack':
                acks.append(dict(ack))
        if self.mode == 'delayed_ack':
            if predicate():
                return
            self.pumps += 1
        self.events.extend(acks)
        if not predicate():
            raise TimeoutError(label)


class GraphAcknowledgements(unittest.TestCase):
    def test_delayed_ack_is_consumed_before_capture_hook(self):
        events = []
        pool = GraphPool(events, 'delayed_ack')
        capture = Mock()
        proof = query_acknowledged_graphs(pool, events, 1, capture_graph=capture)
        self.assertEqual(pool.serial, 2)
        self.assertEqual(pool.pumps, 2)
        self.assertEqual(capture.call_count, 2)
        for role in ('writer', 'reader'):
            self.assertLess(events.index(proof[role]['graph']), events.index(proof[role]['ack']))
            self.assertEqual(proof[role]['graph']['command_id'], proof[role]['ack']['command_id'])

    def test_cached_periodic_and_wrong_ack_do_not_satisfy_query(self):
        for mode in ('no_graph', 'no_ack', 'wrong_command', 'ack_first', 'early_ack', 'duplicate_graph', 'duplicate_ack'):
            with self.subTest(mode=mode):
                events = [{'event': 'graph_snapshot', 'role': role, 'monotonic_ns': 1} for role in ('writer', 'reader')]
                hook = Mock()
                with self.assertRaises(TimeoutError):
                    query_acknowledged_graphs(GraphPool(events, mode), events, 1, capture_graph=hook)
                hook.assert_not_called()


if __name__ == '__main__':
    unittest.main()
