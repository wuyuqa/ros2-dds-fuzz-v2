"""Persisted synthetic raw -> canonical G01 ingest, no DDS/live qualification."""
from copy import deepcopy
import hashlib
import json
from pathlib import Path
import tempfile
import unittest

from r2f.campaign.runtime_adapter import (
    ingest_execution, facts_from_events, canonical_receipts, _primary_rule_id,
)
from r2f.core import compile_scenario
from r2f.core.replay import ReplayCapsule
from r2f.core.evidence import canonical_hash
from r2f.modules.s1.targets import exploratory_target_declarations, target_declarations
from r2f.modules.s1.providers import exploratory_service_bindings
from r2f.modules.s1.oracles import COMPATIBILITY_RULE_ID
import test_g01_relation_runtime_wiring as wiring

TARGET = wiring.TARGET


class G01CanonicalIngest(unittest.TestCase):
    def fixture(self, kind='RR', **overrides):
        runtime, config = wiring.G01RuntimeWiring().run_offline(kind, **overrides)
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        path = Path(temporary.name) / 'raw.jsonl'
        # The runtime double's original directory has been cleaned. Relocate
        # this explicitly synthetic raw while rebuilding its state-lease name.
        for event in runtime['events']:
            if event['event'] == 'isolation_locks_acquired':
                for lock in event['locks']:
                    lock['owner']['state_dir'] = str(path.parent)
                    if Path(lock['path']).name.startswith('state-'):
                        lock['path'] = '/offline/state-' + hashlib.sha256(str(path.parent).encode()).hexdigest() + '.lock'
        self.persist(runtime, path)
        target = exploratory_target_declarations()[TARGET]
        scenario = compile_scenario(target, config['rmw'], config['arm'], config['repeat'])
        source = {'source_repo': str(Path(__file__).resolve().parents[1]),
                  'head': '0' * 40, 'behavior_tree_sha256': '0' * 64,
                  'source_frozen': False, 'dirty_behavior_paths': ['offline_fixture']}
        capsule = ReplayCapsule.from_scenario(scenario, source_manifest=source,
            final_input={k: config[k] for k in ('topic', 'qos', 'writer_qos', 'reader_qos')})
        services = exploratory_service_bindings({TARGET: target})[TARGET]
        return runtime, config, scenario, capsule, services

    def persist(self, runtime, path=None):
        path = Path(path or runtime['raw_path'])
        path.write_text(''.join(json.dumps(e, sort_keys=True) + '\n' for e in runtime['events']))
        runtime['raw_path'] = str(path)
        runtime['raw_sha256'] = hashlib.sha256(path.read_bytes()).hexdigest()

    def ingest(self, fixture):
        runtime, config, scenario, capsule, services = fixture
        return ingest_execution(scenario, config, runtime, capsule, services=services)

    def test_canonical_ingest_all_four_pairs(self):
        for kind in ('RR', 'RB', 'BB', 'BR'):
            with self.subTest(kind=kind):
                fixture = self.fixture(kind, matched=0 if kind == 'BR' else 1)
                row = self.ingest(fixture)
                self.assertEqual(row['rule_id'], COMPATIBILITY_RULE_ID)
                self.assertNotIn('oracle_tool_failure', row)
                self.assertNotIn('ingestion_error', row)
                self.assertEqual(row['execution_stage'], 'mutation_applied' if kind == 'BR' else 'judged', row)
                self.assertEqual(row['oracle_verdict'], 'unknown' if kind == 'BR' else 'pass')
                self.assertFalse(row['candidate_eligible'])
                self.assertFalse(row['seal_eligible'])
                self.assertEqual(len(row['precondition_receipts']), 1)
                self.assertEqual(row['precondition_receipts'][0]['step_id'], 'admit_environment')

    def test_neutral_arms_and_finite_no_match_keep_canonical_stages_honest(self):
        for arm in ('baseline', 'transparent'):
            for matched in (0, 1):
                with self.subTest(arm=arm, matched=matched):
                    row = self.ingest(self.fixture(arm=arm, matched=matched))
                    self.assertEqual(row['execution_stage'], 'judged' if matched else 'reached', row)
                    self.assertEqual(row['oracle_verdict'], 'pass' if matched else 'unknown')
                    self.assertFalse(row['mutation_receipt']['hit'])
                    self.assertFalse(row['candidate_eligible'])
        for kind in ('RR', 'RB', 'BB'):
            row = self.ingest(self.fixture(kind, matched=0))
            self.assertEqual(row['execution_stage'], 'mutation_applied')
            self.assertEqual(row['oracle_verdict'], 'unknown')
            self.assertFalse(row['mutation_receipt']['effect_observed'])
            self.assertFalse(row['candidate_eligible'])

    def test_raw_not_returned_facts_controls_prohibited_match(self):
        fixture = self.fixture('BR', deliver=False)
        runtime = fixture[0]
        runtime['relation_guard_facts'] = {'arbitrary': 'forged_pass'}
        runtime['facts'] = {'match': None, 'oracle': 'pass'}
        runtime['relation_observation'] = {'oracle_input': False, 'facts': {'fake': True}}
        row = self.ingest(fixture)
        self.assertEqual(row['execution_stage'], 'judged', row)
        self.assertEqual(row['oracle_verdict'], 'fail')
        self.assertEqual(row['oracle']['reason_codes'], ['prohibited_match_observed'])
        self.assertFalse(row['candidate_eligible'])  # fixture/unfrozen, not natural SUT candidate
        self.assertFalse(row['g01_source_qualified'])

    def test_existing_manifest_rule_resolves_without_target_mutation(self):
        before = exploratory_target_declarations()[TARGET].to_dict()
        fixture = self.fixture()
        self.assertEqual(_primary_rule_id(fixture[2]), COMPATIBILITY_RULE_ID)
        self.assertEqual(canonical_hash(before), canonical_hash(fixture[2].target.to_dict()))
        self.assertNotIn('primary_rule', before['rule_contract'])
        required = target_declarations()
        for target in required.values():
            scenario = compile_scenario(target, 'rmw_cyclonedds_cpp', 'mutated', 0)
            self.assertEqual(_primary_rule_id(scenario), target.rule_contract['primary_rule'])

    def test_bad_compatibility_rule_is_not_inferred_from_oracle(self):
        fixture = list(self.fixture())
        manifest = fixture[2].target.to_dict()
        manifest['rule_contract']['compatibility_rule'] = 'foreign.rule'
        fixture[2] = compile_scenario(manifest, fixture[1]['rmw'], 'mutated', 0)
        row = self.ingest(fixture)
        self.assertEqual(row['execution_stage'], 'aborted')
        self.assertTrue(row['tool_error'])
        self.assertFalse(row['candidate_eligible'])

    def test_missing_observation_yields_unknown_without_fake_effect(self):
        fixture = self.fixture()
        runtime = fixture[0]
        runtime['events'] = [e for e in runtime['events'] if e['event'] != 'match_snapshot']
        for i, event in enumerate(runtime['events']):
            event['event_index'] = i
        # Raw end cannot self-certify the removed relation effect.
        runtime['mutation_receipt']['effect_observed'] = False
        next(e for e in runtime['events'] if e['event'] == 'runtime_end')['mutation_receipt'] = deepcopy(runtime['mutation_receipt'])
        self.persist(runtime)
        row = self.ingest(fixture)
        self.assertEqual(row['oracle_verdict'], 'unknown')
        self.assertFalse(row['candidate_eligible'])
        self.assertNotEqual(row['execution_stage'], 'judged')
        self.assertFalse(row['typed_facts']['relation_effect_observed'])

    def test_missing_precondition_raw_or_forged_scope_cannot_reach_canonical(self):
        for damage in ('scope', 'begin_ref', 'begin_time', 'metadata_receipt', 'lease_owner'):
            with self.subTest(damage=damage):
                fixture = self.fixture()
                runtime = fixture[0]
                events = runtime['events']
                if damage == 'scope':
                    next(e for e in events if e['event'] == 'isolation_locks_acquired')['locks'].pop()
                elif damage == 'lease_owner':
                    next(e for e in events if e['event'] == 'isolation_locks_acquired')['locks'][0]['owner']['domain_id'] = 172
                elif damage == 'begin_ref':
                    next(e for e in events if e['event'] == 'mutation_begin')['precondition_receipt_refs'][0]['raw_after_index'] = 0
                elif damage == 'begin_time':
                    next(e for e in events if e['event'] == 'mutation_begin')['monotonic_ns'] = 0
                else:
                    runtime['precondition_receipts'][0]['after']['satisfied'] = False
                self.persist(runtime)
                row = self.ingest(fixture)
                self.assertEqual(row['execution_stage'], 'aborted')
                self.assertIn('ingestion_error', row)
                self.assertFalse(row['candidate_eligible'])
                self.assertEqual(row['oracle_verdict'], 'unknown')

    def test_forged_effect_receipt_and_bad_raw_hash_are_rejected(self):
        for damage in ('effect', 'hash'):
            with self.subTest(damage=damage):
                fixture = self.fixture('BR', matched=0)
                runtime = fixture[0]
                if damage == 'effect':
                    runtime['mutation_receipt']['effect_observed'] = True
                    next(e for e in runtime['events'] if e['event'] == 'runtime_end')['mutation_receipt'] = deepcopy(runtime['mutation_receipt'])
                    self.persist(runtime)
                else:
                    runtime['raw_sha256'] = 'f' * 64
                row = self.ingest(fixture)
                self.assertEqual(row['execution_stage'], 'aborted')
                self.assertFalse(row['candidate_eligible'])

    def test_process_exit_error_is_not_a_candidate(self):
        fixture = self.fixture('BR')
        runtime = fixture[0]
        next(e for e in runtime['events'] if e['event'] == 'process_exit')['returncode'] = 1
        self.persist(runtime)
        row = self.ingest(fixture)
        self.assertEqual(row['execution_stage'], 'aborted')
        self.assertFalse(row['candidate_eligible'])


if __name__ == '__main__':
    unittest.main()
