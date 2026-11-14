"""Identity evidence contracts; these synthetic observations never create execution metadata."""
import unittest
from r2f.runtime.ros2_pair import summarize_events


def correlated_events():
    events = [
        {'event': 'endpoint_created', 'role': 'writer', 'pid': 101, 'endpoint_gid': 'writer',
         'run_id': 'r', 'created_monotonic_ns': 1},
        {'event': 'endpoint_created', 'role': 'reader', 'pid': 102, 'endpoint_gid': 'reader',
         'run_id': 'r', 'created_monotonic_ns': 2},
        {'event': 'graph_snapshot', 'role': 'writer', 'monotonic_ns': 3,
         'publishers': [{'endpoint_gid': 'writer'}], 'subscriptions': [{'endpoint_gid': 'reader'}]},
        {'event': 'graph_snapshot', 'role': 'reader', 'monotonic_ns': 4,
         'publishers': [{'endpoint_gid': 'writer'}], 'subscriptions': [{'endpoint_gid': 'reader'}]},
        {'event': 'window_open', 'window': 'post_match_delivery', 'monotonic_ns': 5},
        {'event': 'delivery_contract', 'expected_sequences': [1, 2, 3]},
        {'event': 'send', 'phase': 'post_match', 'send_sequence': 1},
        {'event': 'send', 'phase': 'post_match', 'send_sequence': 2},
        {'event': 'send', 'phase': 'post_match', 'send_sequence': 3},
        {'event': 'receive', 'phase': 'post_match', 'send_sequence': 1, 'payload_run_id': 'r', 'publisher_gid': None},
        {'event': 'receive', 'phase': 'post_match', 'send_sequence': 3, 'payload_run_id': 'r', 'publisher_gid': None},
        {'event': 'window_close', 'window': 'post_match_delivery', 'monotonic_ns': 8},
        {'event': 'graph_snapshot', 'role': 'writer', 'monotonic_ns': 9,
         'publishers': [{'endpoint_gid': 'writer'}], 'subscriptions': [{'endpoint_gid': 'reader'}]},
        {'event': 'graph_snapshot', 'role': 'reader', 'monotonic_ns': 10,
         'publishers': [{'endpoint_gid': 'writer'}], 'subscriptions': [{'endpoint_gid': 'reader'}]},
    ]
    for i, event in enumerate(events):
        event['event_index'] = i
    return events


class RuntimeIdentity(unittest.TestCase):
    def test_absent_api_is_not_verified_or_mismatch(self):
        facts = summarize_events(correlated_events())
        self.assertEqual(facts['publisher_gid_status'], 'unavailable_api')
        self.assertIsNone(facts['publisher_gid_consistent'])
        self.assertTrue(facts['endpoint_identity_correlated'])
        self.assertEqual(facts['identity_basis'], 'endpoint_correlated')
        self.assertEqual(facts['sut_missing_with_later_sentinel'], [2])

    def test_actual_direct_gid_mismatch_is_distinct(self):
        events = correlated_events()
        for event in events:
            if event['event'] == 'receive':
                event['publisher_gid'] = 'unrelated-writer'
        facts = summarize_events(events)
        self.assertEqual(facts['publisher_gid_status'], 'mismatch')
        self.assertFalse(facts['publisher_gid_consistent'])

    def test_graph_change_invalidates_correlated_identity(self):
        events = correlated_events()
        events[-1]['publishers'].append({'endpoint_gid': 'intruder'})
        facts = summarize_events(events)
        self.assertFalse(facts['graph_stable_across_window'])
        self.assertFalse(facts['endpoint_identity_correlated'])

    def test_lack_of_trailing_graph_invalidates_correlation(self):
        facts = summarize_events(correlated_events()[:-2])
        self.assertFalse(facts['endpoint_identity_correlated'])

    def test_no_sentinel_does_not_claim_sut_missing_witness(self):
        events = [e for e in correlated_events() if not (e['event'] == 'receive' and e['send_sequence'] == 3)]
        self.assertEqual(summarize_events(events)['sut_missing_with_later_sentinel'], [])

    def test_no_receive_does_not_claim_direct_identity_complete(self):
        self.assertFalse(summarize_events([])['publisher_gid_evidence_complete'])
        self.assertIsNone(summarize_events([])['publisher_gid_consistent'])


if __name__ == '__main__':
    unittest.main()
