"""Read-only third-target development ingest; no ROS process or seal claim.

Missing live artifacts skip only integration cases. Independent synthetic unit
contracts live in test_campaign_recovery.py and always run.
"""
import copy
import json
from pathlib import Path
import tempfile
import unittest

from r2f.campaign.runtime_adapter import ingest_execution
from r2f.campaign.triage import attribute
from r2f.core import ReplayCapsule, compile_scenario, raw_sha256
from r2f.modules.s1.module import S1Module
from r2f.modules.s1.targets import EXECUTION_ORDER

ROOT = Path(__file__).resolve().parents[1]
THIRD = EXECUTION_ORDER[2]


def smoke_inputs(path):
    """Reconstruct only semantics witnessed by saved config and raw receipts."""
    runtime = json.loads(path.read_text())
    events, metadata = runtime['events'], runtime['execution_metadata']
    source_path = path.parent.parent / (path.parent.name + '-config.json')
    original = json.loads(source_path.read_text())
    writer = next(e for e in events if e['event'] == 'endpoint_created' and e['role'] == 'writer')
    delay = next(e for e in events if e['event'] == 'reader_creation_delay_begin')
    contract = next(e for e in events if e['event'] == 'delivery_contract')
    prejoin = next(e for e in events if e['event'] == 'prejoin_publish_complete')
    parameters = {key: original[key] for key in ('drop_duration_sec', 'recovery_timeout_sec')}
    # This polling interval is not a delivery oracle fact. If omitted by the
    # development CLI it comes from this reviewed runtime's explicit default.
    parameters['counter_interval_sec'] = original.get('counter_interval_sec', .25)
    submitted = dict(parameters, topic=writer['topic'], message_type=writer['message_type'],
                     qos=writer['qos'], reader_delay_sec=delay['configured_delay_sec'],
                     pre_match_count=prejoin['count'], post_match_count=len(contract['expected_sequences']),
                     fault_id=contract.get('fault_id'))
    config = dict(original, **submitted, repeat=metadata['repeat'])
    if config['arm'] == 'negative':
        config['control_kind'] = 'fault'
    scenario = compile_scenario(S1Module().targets[THIRD], metadata['rmw'], metadata['arm'], metadata['repeat'],
                                parameters={'mutation_kind': 'discovery_drop', 'carrier': 'wire_proxy',
                                            'mutation_parameters': parameters})
    capsule = ReplayCapsule.from_scenario(scenario, source_manifest=metadata['source'],
                                         final_input=submitted, origin_domain=metadata['domain_id'])
    return scenario, config, runtime, capsule


class ExistingRecoverySmoke(unittest.TestCase):
    def artifacts(self):
        paths = sorted((ROOT / 'results/runtime-smoke/third-development-v4').glob('*/runtime_result.json'))
        paths += sorted((ROOT / 'results/runtime-smoke').glob('third-negative-*/*/runtime_result.json'))
        return [p for p in paths if json.loads(p.read_text()).get('runtime_status') == 'completed']

    def test_real_capture_rebuild_reaches_core_and_records_capsule(self):
        paths = self.artifacts()
        if not paths:
            self.skipTest('third development smoke evidence not present')
        for path in paths:
            with self.subTest(path=str(path)):
                scenario, config, runtime, capsule = smoke_inputs(path)
                row = ingest_execution(scenario, config, runtime, capsule)
                self.assertEqual(row['execution_stage'], 'judged', row.get('abort_reason'))
                self.assertNotIn('evidence_error', row['typed_facts'])
                self.assertEqual(len(row['precondition_receipts']), 3)
                self.assertEqual(len(row['observation_receipts']), 2)
                self.assertTrue(row['precondition_receipts'][0]['requires']['namespace_ready'])
                self.assertFalse(row['typed_facts']['identity_proof']['direct_gid_match'])
                expected = 'fail' if config['arm'] == 'negative' else 'pass'
                self.assertEqual(row['oracle_verdict'], expected)
                capsule.record_execution(row)
                copy_capsule = ReplayCapsule.from_dict(capsule.to_dict())
                self.assertEqual(copy_capsule.execution['typed_facts'], row['typed_facts'])
                if config['arm'] == 'negative':
                    self.assertFalse(row['mutation_receipt']['hit'])
                    self.assertNotIn('mutation_applied', [s['execution_stage'] for s in row['stage_history']])
                    self.assertEqual(row['fault_control_receipt']['receipt_kind'], 'delivery_omission')
                    self.assertEqual(attribute(row, row['typed_facts'], row['oracle'])['responsibility_layer'], 'harness')

    def test_runtime_summary_does_not_replace_independent_packet_evidence(self):
        paths = self.artifacts()
        if not paths:
            self.skipTest('third development smoke evidence not present')
        scenario, config, runtime, capsule = smoke_inputs(paths[0])
        runtime['facts'] = {'made_up_summary': True}
        row = ingest_execution(scenario, config, runtime, capsule)
        self.assertEqual(row['execution_stage'], 'judged', row.get('abort_reason'))
        self.assertEqual(row['oracle_verdict'], 'pass')

    def test_raw_rehashed_tamper_cannot_substitute_for_capture_or_fresh_counts(self):
        paths = self.artifacts()
        if not paths:
            self.skipTest('third development smoke evidence not present')
        scenario, config, original, capsule = smoke_inputs(paths[0])
        original_bytes = Path(original['raw_path']).read_bytes()
        for mode in ('pcap_hash', 'packet_index_hash', 'activity', 'boundary', 'count', 'counter', 'empty_publications'):
            with self.subTest(mode=mode):
                runtime = copy.deepcopy(original)
                events = runtime['events']
                if mode in ('pcap_hash', 'packet_index_hash'):
                    stop = next(e['receipt'] for e in events if e['event'] == 'discovery_observer_stopped')
                    stop['raw' if mode == 'pcap_hash' else 'index']['sha256'] = '0' * 64
                elif mode == 'activity':
                    for e in events:
                        if e['event'] == 'discovery_activity_observation' and e.get('window') == 'post_restore':
                            e['activity']['events'] = []
                elif mode == 'boundary':
                    e = next(e for e in events if e['event'] == 'discovery_restore_boundary')
                    e['activity_since_unix_ns'] = e['post_restore_unix_ns'] - 1
                elif mode == 'count':
                    boundary = next(e for e in events if e['event'] == 'discovery_restore_boundary')
                    e = next(e for e in events if e['event'] == 'match_snapshot' and e['monotonic_ns'] > boundary['monotonic_ns'])
                    e['count_api'] = 'cached_count'
                elif mode == 'counter':
                    e = next(e for e in events if e['event'] == 'discovery_drop_counter')
                    e['receipt']['packets'] = -1
                else:
                    for e in events:
                        if e['event'] == 'send' and e.get('phase') == 'post_restore':
                            e['phase'] = 'unrelated'
                # A new side artifact within the same artifact root keeps the
                # independent pcap paths in scope. Never overwrite the old raw.
                with tempfile.NamedTemporaryFile(dir=Path(original['raw_path']).parent, prefix='offline-audit-', suffix='.jsonl') as temp:
                    Path(temp.name).write_text(''.join(json.dumps(e) + '\n' for e in events))
                    runtime['raw_path'] = temp.name
                    runtime['raw_sha256'] = raw_sha256(temp.name)
                    row = ingest_execution(scenario, config, runtime, capsule)
                self.assertEqual(row['execution_stage'], 'aborted')
                self.assertEqual(row['oracle_verdict'], 'unknown')
                self.assertFalse(row['typed_facts']['endpoint_identity_complete'])
        self.assertEqual(Path(original['raw_path']).read_bytes(), original_bytes)
