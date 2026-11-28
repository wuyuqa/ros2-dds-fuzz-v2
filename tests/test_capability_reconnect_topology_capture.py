"""Read-only authentic saved topology admission; no live backend invocation."""
import copy
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from r2f.campaign.runtime_adapter import rebuild_saved_case_facts
from r2f.core.replay import ReplayCapsule
from r2f.modules.s1.module import resolve_services

PACKAGE = Path('/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-issue6-implementation-20261121/issue10-normal-recovery-fix-blackbox-v7-pmapsv2-prepared/original')
CASE = 's1-issue10-bec6618-blackbox-d116-20261122.0000.S1.topology.program'


@unittest.skipUnless(PACKAGE.is_dir(), 'authentic read-only saved package unavailable')
class TopologyCaptureTests(unittest.TestCase):
    def setUp(self):
        folder = PACKAGE / 'cases' / CASE
        self.row = json.loads((folder / 'row.json').read_text())
        self.runtime = json.loads(Path(self.row['runtime_result_path']).read_text())
        self.capsule = ReplayCapsule.from_dict(json.loads((folder / 'capsule.json').read_text()))
        self.case = {'row': self.row, 'input_sha256': self.row['effective_input_sha256']}
        self.services = resolve_services(self.row['target_id'], exploratory=True,
                                         runtime_capability='topology_v1')
        # Any accidental execution at these public runtime seams fails the test.
        for name in ('r2f.runtime.ros2_pair.run_pair',
                     'r2f.runtime.topology_pair.run_topology',
                     'r2f.runtime.topology_namespace.run_isolated_topology',
                     'r2f.runtime.domains.scan_processes',
                     'r2f.runtime.domains.scan_old_locks'):
            guard = patch(name, side_effect=AssertionError('offline runtime tripwire'))
            guard.start()
            self.addCleanup(guard.stop)

    def rebuild(self):
        return rebuild_saved_case_facts(self.case, runtime=self.runtime,
                                        capsule=self.capsule, services=self.services)

    def test_authentic_normal_capture_reconstructs_without_rewriting_inputs(self):
        before = copy.deepcopy((self.case, self.runtime, self.capsule.to_dict()))
        scenario, facts = self.rebuild()
        self.assertEqual(scenario.target_id, 'discovery.endpoint_topology.fuzz_v1')
        self.assertTrue(facts['diagnostic_valid'])
        self.assertTrue(facts['source_provenance_qualified'])
        self.assertTrue(facts['process_healthy'])
        self.assertTrue(facts['endpoint_identity_complete'])
        self.assertEqual((self.case, self.runtime, self.capsule.to_dict()), before)

    def sync_metadata(self):
        self.row['runtime_execution_metadata'] = copy.deepcopy(self.runtime['execution_metadata'])

    def reject(self, reason):
        with self.assertRaisesRegex(ValueError, reason):
            self.rebuild()

    def test_saved_capture_does_not_reopen_historical_source_checkout(self):
        with patch('r2f.campaign.identity.source_manifest',
                   side_effect=AssertionError('historical source checkout must not be measured')):
            _, facts = self.rebuild()
        self.assertTrue(facts['source_provenance_qualified'])
        self.assertTrue(facts['preconditions_reached'])

    def test_default_producer_still_requires_current_source_after_capture(self):
        from r2f.campaign.topology_adapter import TopologyFactProducer
        from r2f.campaign.runtime_adapter import _saved_topology_capture
        from r2f.core.scenario import Scenario
        capture_source = self.row['source_manifest']
        config, events = _saved_topology_capture(
            self.row, self.runtime, self.capsule.input,
            Scenario.from_dict(self.capsule.input['scenario']), capture_source)
        source = dict(capture_source, behavior_tree_sha256='0' * 64)
        # JSON data cannot select the explicit archival Python argument.
        config = dict(config, capture_source=capture_source, saved_capture=True)
        evidence = {'events': events, 'runtime': self.runtime, 'config': config}
        with patch('r2f.campaign.identity.source_manifest', return_value=source):
            facts = TopologyFactProducer().produce(evidence)
        self.assertFalse(facts['source_provenance_qualified'])
        self.assertFalse(facts['preconditions_reached'])
        self.assertFalse(facts['candidate_eligible'])

    def test_explicit_archival_source_must_match_the_bound_capture(self):
        from r2f.campaign.runtime_adapter import _saved_topology_capture
        from r2f.core.scenario import Scenario
        source = self.row['source_manifest']
        config, events = _saved_topology_capture(
            self.row, self.runtime, self.capsule.input,
            Scenario.from_dict(self.capsule.input['scenario']), source)
        evidence = {'events': events, 'runtime': self.runtime, 'config': config}
        facts = self.services.fact_producer.produce(
            evidence, capture_source=dict(source, head='0' * 40))
        self.assertFalse(facts['source_provenance_qualified'])
        self.assertFalse(facts['preconditions_reached'])
        self.assertFalse(facts['candidate_eligible'])

    def test_source_after_drift_cannot_be_hidden_by_row_metadata(self):
        self.runtime['execution_metadata']['source_after']['behavior_tree_sha256'] = '0' * 64
        self.sync_metadata()
        self.reject('source_identity_mismatch')

    def test_boolean_source_frozen_is_required(self):
        self.runtime['execution_metadata']['source_manifest']['source_frozen'] = 1
        self.sync_metadata()
        self.reject('source_identity_mismatch')

    def test_unknown_capture_version_fails_closed(self):
        self.runtime['execution_metadata']['schema'] = 'r2f.topology-execution-metadata/v2'
        self.sync_metadata()
        self.reject('unsupported_capture_schema')

    def test_row_metadata_is_an_independent_binding(self):
        self.row['runtime_execution_metadata']['workers'][0]['pid'] += 1
        self.reject('metadata_mismatch')

    def test_context_boolean_is_not_revision_zero_or_repeat_zero(self):
        for key in ('target_revision', 'repeat', 'domain_id'):
            with self.subTest(key=key):
                old = self.runtime['execution_metadata'][key]
                self.runtime['execution_metadata'][key] = False
                self.sync_metadata()
                self.reject('context_mismatch|context_integer_invalid')
                self.runtime['execution_metadata'][key] = old

    def test_sealed_input_and_plan_identity_are_not_inferred(self):
        for key in ('effective_input_sha256', 'plan_hash'):
            with self.subTest(key=key):
                saved = copy.deepcopy(self.runtime['execution_metadata'])
                self.runtime['execution_metadata']['sealed_execution_context'][key] = '0' * 64
                self.sync_metadata()
                self.reject('sealed_context_mismatch')
                self.runtime['execution_metadata'] = saved

    def test_worker_pid_start_generation_and_endpoint_are_joined_to_raw(self):
        for key, value in (('pid', 99999), ('process_start_ticks', 99999),
                           ('generation', 3), ('endpoint_gid', 'alien-guid'),
                           ('endpoint_generation', 'alien-generation')):
            with self.subTest(key=key):
                saved = copy.deepcopy(self.runtime['execution_metadata'])
                self.runtime['execution_metadata']['workers'][0][key] = value
                self.sync_metadata()
                self.reject('worker_process_binding_mismatch|worker_generation_unbound')
                self.runtime['execution_metadata'] = saved

    def test_worker_boolean_values_do_not_equal_integer_identity(self):
        for key in ('pid', 'pgid', 'process_start_ticks', 'generation', 'worker_generation', 'returncode'):
            with self.subTest(key=key):
                saved = copy.deepcopy(self.runtime['execution_metadata'])
                self.runtime['execution_metadata']['workers'][0][key] = (False if key == 'returncode' else True)
                self.sync_metadata()
                self.reject('worker_identity_mismatch|worker_generation_unbound')
                self.runtime['execution_metadata'] = saved

    def test_duplicate_or_missing_worker_generation_fails_closed(self):
        for duplicate in (True, False):
            with self.subTest(duplicate=duplicate):
                saved = copy.deepcopy(self.runtime['execution_metadata'])
                workers = self.runtime['execution_metadata']['workers']
                if duplicate:
                    workers.append(copy.deepcopy(workers[0]))
                else:
                    workers.pop()
                self.sync_metadata()
                with self.assertRaisesRegex(ValueError, 'worker|bindings'):
                    self.rebuild()
                self.runtime['execution_metadata'] = saved

    def test_namespace_pid_cannot_be_interpreted_as_host_identity(self):
        self.runtime['execution_metadata']['workers'][0]['pid_namespace'] = 'pid:[wrong]'
        self.sync_metadata()
        self.reject('worker_namespace_mismatch')

    def test_namespace_receipts_cannot_be_substituted(self):
        self.runtime['topology_namespace']['worker_bindings'][0]['process_start_ticks'] += 1
        self.reject('namespace_manifest_mismatch')

    def test_unhashed_raw_endpoint_generation_edits_fail(self):
        token = next(e for e in self.runtime['events'] if e['event'] == 'endpoint_created')
        token['generation'] = 99
        self.reject('raw_events_mismatch')

    def test_raw_file_bytes_must_match_hash(self):
        with tempfile.TemporaryDirectory(dir='/dev/shm', prefix='cp3-raw-') as scratch:
            raw = Path(scratch) / 'raw.jsonl'
            raw.write_bytes(Path(self.row['raw_path']).read_bytes() + b'\n')
            self.row['raw_path'] = self.runtime['raw_path'] = str(raw)
            self.runtime['execution_metadata']['raw_path'] = str(raw)
            self.sync_metadata()
            self.reject('SHA256|hash')

    def test_captured_fault_marker_is_rejected(self):
        self.runtime['execution_metadata']['fault_id'] = 'controlled-middle-omission'
        self.sync_metadata()
        self.reject('control_contamination')

    def test_negative_and_fault_control_rows_are_rejected(self):
        self.row['control_kind'] = 'fault'
        self.reject('control_contamination')

    def test_row_control_provenance_cannot_be_laundered(self):
        self.row['candidate_source'] = 'control'
        self.reject('control_contamination')

    def test_hash_valid_request_outside_capture_root_is_rejected(self):
        from r2f.core.evidence import raw_sha256
        with tempfile.TemporaryDirectory(dir='/dev/shm', prefix='cp3-sidecar-') as scratch:
            ns = self.runtime['topology_namespace']
            manifest = json.loads(Path(ns['manifest_path']).read_text())
            request = Path(scratch) / 'request.json'
            request.write_bytes(Path(manifest['request_ref']['path']).read_bytes())
            manifest['request_ref'].update(path=str(request), sha256=raw_sha256(request))
            path = Path(scratch) / 'manifest.json'
            path.write_text(json.dumps(manifest))
            ns.update(manifest_path=str(path), manifest_sha256=raw_sha256(path))
            self.reject('artifact_path_or_hash_mismatch')

    def test_saved_environment_cannot_disagree_with_measured_artifact(self):
        self.runtime['environment']['ok'] = False
        self.reject('environment_artifact_mismatch|inner_result_mismatch')

    def test_unrelated_runtime_payload_cannot_borrow_namespace_manifest(self):
        self.runtime['backend'] = 'offline_fake'
        self.reject('inner_result_mismatch')

    def test_recorded_typed_facts_are_not_reconstruction_inputs(self):
        self.row['typed_facts'] = {'diagnostic_valid': False, 'process_healthy': False}
        _, facts = self.rebuild()
        self.assertTrue(facts['diagnostic_valid'])
        self.assertTrue(facts['process_healthy'])

    def test_rehashed_parent_descriptor_must_match_saved_request_outer(self):
        with self.derived_parent_manifest() as ns:
            ns['outer_controller']['namespaces']['net'] = 'net:[unrelated]'
        self.reject('namespace_request_outer_mismatch')

    def derived_parent_manifest(self):
        """Change only copied parent claims; independent capture stays original."""
        from contextlib import contextmanager
        from r2f.core.evidence import raw_sha256

        @contextmanager
        def persistent():
            scratch = tempfile.TemporaryDirectory(dir='/dev/shm', prefix='cp3-parent-proof-')
            self.addCleanup(scratch.cleanup)
            ns = self.runtime['topology_namespace']
            manifest = json.loads(Path(ns['manifest_path']).read_text())
            yield ns
            for key in ('outer_controller', 'nested_controller_binding'):
                manifest[key] = copy.deepcopy(ns[key])
            path = Path(scratch.name) / 'manifest.json'
            path.write_text(json.dumps(manifest))
            ns.update(manifest_path=str(path), manifest_sha256=raw_sha256(path))
        return persistent()

    def test_rehashed_parent_binding_requires_canonical_authenticated_proof(self):
        with self.derived_parent_manifest() as ns:
            ns['nested_controller_binding']['authentication'] = 'UNAUTHENTICATED_FIXTURE'
        self.reject('controller_binding_schema_invalid')

    def test_rehashed_reported_inner_namespace_must_join_parent_observation(self):
        with self.derived_parent_manifest() as ns:
            ns['nested_controller_binding']['reported_inner']['namespaces']['pid'] = 'pid:[unrelated]'
        self.reject('controller_identity_mismatch')

    def test_rehashed_parent_pid_scope_must_join_request_outer_namespace(self):
        with self.derived_parent_manifest() as ns:
            ns['nested_controller_binding']['parent_pid_scope'] = 'pid:[unrelated]'
        self.reject('controller_pid_chain_mismatch')

    def test_parent_binding_unknown_or_missing_schema_fails_closed(self):
        original = copy.deepcopy(self.runtime)
        for field in ('authentication', 'parent_pid_scope', 'outer', 'reported_inner',
                      'inner_pid', 'pid_scope', 'pid_namespace_chain', 'unknown_schema'):
            with self.subTest(field=field):
                self.runtime = copy.deepcopy(original)
                with self.derived_parent_manifest() as ns:
                    binding = ns['nested_controller_binding']
                    if field == 'unknown_schema':
                        binding['schema'] = 'r2f.unrecognized-binding/v99'
                    else:
                        del binding[field]
                self.reject('controller_binding_schema_invalid')

    def test_parent_binding_requires_typed_controller_pid_and_start_relationships(self):
        original = copy.deepcopy(self.runtime)
        mutations = (
            ('inner_pid', True), ('outer.pid', True), ('outer.nspid', [25]),
            ('reported_inner.pid', 2), ('reported_inner.nspid', [2]),
            ('outer.uid', 0), ('reported_inner.uid', 1000),
            ('pid_namespace_chain', []), ('reported_inner.start_ticks', 1),
        )
        for field, value in mutations:
            with self.subTest(field=field):
                self.runtime = copy.deepcopy(original)
                with self.derived_parent_manifest() as ns:
                    binding = ns['nested_controller_binding']
                    parts = field.split('.')
                    target = binding if len(parts) == 1 else binding[parts[0]]
                    target[parts[-1]] = value
                self.reject('controller_identity_mismatch|controller_pid_chain_mismatch')

    def test_partial_or_nonseparated_controller_namespace_proof_fails_closed(self):
        original = copy.deepcopy(self.runtime)
        for mutation in ('missing_map', 'invalid_map', 'missing_capability', 'boolean_capability',
                         'missing_namespace', 'untyped_namespace', 'shared_namespace', 'negative_capability'):
            with self.subTest(mutation=mutation):
                self.runtime = copy.deepcopy(original)
                with self.derived_parent_manifest() as ns:
                    for key in ('outer', 'reported_inner'):
                        identity = ns['nested_controller_binding'][key]
                        if mutation == 'missing_namespace':
                            del identity['namespaces']['net']
                        elif mutation == 'untyped_namespace':
                            identity['namespaces']['net'] = 'untyped'
                        elif mutation == 'shared_namespace':
                            identity['namespaces']['net'] = ns['outer_controller']['namespaces']['net']
                        elif mutation == 'missing_map':
                            del identity['uid_map']
                        elif mutation == 'invalid_map':
                            identity['gid_map'] = 'invalid'
                        else:
                            identity['cap_eff'] = (True if mutation == 'boolean_capability' else
                                                   -1 if mutation == 'negative_capability' else 0)
                self.reject('controller_identity_mismatch|worker_process_binding_mismatch')
