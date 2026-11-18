"""Pure file fixtures: never create namespaces, processes, leases or DDS/ROS.

Every positive fixture below is fabricated structure, NOT execution evidence.
"""
import copy
import hashlib
import json
from pathlib import Path
import tempfile
import unittest
from unittest import mock

from r2f.campaign.boundary_audit import audit_execution_boundary, BOUNDARY_SCHEMA, AUTHENTICATION, CLEANUP_METHOD, CLEARANCE_SCOPE, PID_INTERPRETATION
from r2f.runtime.ros2_pair import normalize_config

ROOT = Path(__file__).resolve().parents[1]


def canonical(value, *, ensure_ascii=True):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False, ensure_ascii=ensure_ascii).encode()


def digest(value, **kwargs):
    return hashlib.sha256(canonical(value, **kwargs)).hexdigest()


def write(path, value):
    path.write_text(json.dumps(value, sort_keys=True, indent=2) + '\n')


def process(pid, inner_pid=None, *, isolated=False):
    return {'pid': pid, 'start_ticks': 100 + pid, 'uid': 1000, 'cap_eff': 0,
            'namespaces': {name: name + ':[' + str(index + (101 if isolated else 1)) + ']'
                           for index, name in enumerate(('user', 'mnt', 'pid', 'net'))},
            'nspid': [pid, inner_pid] if isolated else [pid],
            'uid_map': '1000 1000 1\n' if isolated else '0 0 4294967295\n',
            'gid_map': '1000 1000 1\n' if isolated else '0 0 4294967295\n'}


def binding(pid=2001, inner_pid=1):
    identity = process(pid, inner_pid, isolated=True)
    return {'authentication': AUTHENTICATION, 'host': identity,
            'inner_pid': inner_pid, 'pid_scope': identity['namespaces']['pid'],
            'loopback_report': {'up': True, 'flags': 73, 'interfaces': [[1, 'lo']]}}


class SyntheticBoundary:
    """Serialize only existing v1 writer fields, never invent a live witness."""
    def __init__(self, root):
        self.root = root
        self.boundary_id = 'a' * 32
        self.directory = root / ('execution-boundary-' + self.boundary_id)
        self.directory.mkdir()
        (root / 'inner-runtime').mkdir()
        self.host = process(1000)
        self.controller = binding()
        self.guardian = process(1001)
        source = {'source_repo': '/synthetic-not-a-real-repository', 'source_frozen': True,
                  'head': 'a' * 40, 'behavior_tree_sha256': 'b' * 64, 'dirty_behavior_paths': []}
        self.config = {'run_id': 'synthetic-offline', 'campaign_id': 'synthetic-fixture',
                       'domain_id': 181, 'source_manifest': source,
                       'source_repo': source['source_repo'], 'expected_head': source['head'],
                       'lock_root': str(root / 'locks'), 'execution_boundary': {'enabled': True},
                       'scenario': {'target': {'name': 'synthetic-not-a-registered-target', 'note': '离线结构'}},
                       'effective_input_sha256': 'd' * 64}
        self.normalized = normalize_config(copy.deepcopy(self.config))
        self.lock_root = Path(self.config['lock_root'])
        self.inner_root = self.lock_root / 'execution-boundaries' / self.boundary_id / 'inner'
        self.host_leases = self.leases(self.lock_root, root, 1000, host=True)
        self.inner_leases = self.leases(self.inner_root, root / 'inner-runtime', 1)
        self.cleanup = {'confirmed': True, 'pid1_exited': True, 'launcher_exited': True,
                        'launcher_returncode': 0, 'descendants': [{'binding': binding(2002, 34), 'pidfd_exited': True}],
                        'method': CLEANUP_METHOD, 'monotonic_ns': 2000}
        identity = {'config_sha256': digest(self.config), 'source_manifest': copy.deepcopy(source),
                    'target_sha256': digest(self.config['scenario']['target'], ensure_ascii=False),
                    'effective_input_sha256': self.config['effective_input_sha256'], 'dependency_lock_sha256': 'e' * 64}
        inner_config = dict(self.normalized, lock_root=str(self.inner_root))
        self.request = {'schema': BOUNDARY_SCHEMA, 'boundary_id': self.boundary_id,
                        'host': copy.deepcopy(self.host), 'config': inner_config,
                        'submitted_config_sha256': digest(self.normalized), 'inner_config_sha256': digest(inner_config),
                        'options': {'timeout_sec': 180.0, 'term_grace_sec': 2.0, 'kill_wait_sec': 10.0, 'startup_timeout_sec': 15.0},
                        'output_dir': str(root), 'audit_dir': str(self.directory), 'identity': copy.deepcopy(identity),
                        'probe': None, 'host_clearance_claimed': False, 'clearance_scope': CLEARANCE_SCOPE,
                        'host_leases': copy.deepcopy(self.host_leases)}
        self.raw = [{'event': 'isolation_locks_acquired', 'event_index': 0, 'role': 'controller',
                     'pid': 1, 'pgid': 1, 'run_id': self.config['run_id'], 'domain_id': 181,
                     'monotonic_ns': 90, 'locks': copy.deepcopy(self.inner_leases)},
                    {'event': 'runtime_end', 'event_index': 1, 'pid': 1, 'pgid': 1,
                     'run_id': self.config['run_id'], 'domain_id': 181, 'monotonic_ns': 100}]
        self.raw_path = root / 'inner-runtime/raw.jsonl'
        self.raw_path.write_bytes(b''.join(canonical(row) + b'\n' for row in self.raw))
        self.inner = {'events': copy.deepcopy(self.raw), 'facts': {'only': 'fabricated fixture'},
                      'real_execution': False, 'status': 'synthetic', 'runtime_status': 'synthetic',
                      'raw_path': str(self.raw_path), 'raw_sha256': hashlib.sha256(self.raw_path.read_bytes()).hexdigest(),
                      'execution': {'workers': [{'pid': 35, 'returncode': 0}], 'domain_id': 181},
                      'execution_metadata': {'workers': [{'pid': 35, 'returncode': 0}], 'domain_id': 181}}
        self.manifest = {'schema': BOUNDARY_SCHEMA, 'boundary_id': self.boundary_id, 'identity': identity,
                         'host_supervisor': copy.deepcopy(self.host), 'guardian': self.guardian,
                         'controller_binding': copy.deepcopy(self.controller),
                         'host_leases': copy.deepcopy(self.host_leases), 'inner_lock_root': str(self.inner_root),
                         'owned_process_observations': [],
                         'owned_process_observation_limitation': 'sampled supporting mapping, not complete lifecycle/host clearance',
                         'inner_leases': copy.deepcopy(self.inner_leases), 'cleanup': self.cleanup, 'error': None,
                         'nonDDS': False, 'host_clearance_claimed': False, 'clearance_scope': CLEARANCE_SCOPE,
                         'source_freeze_credit': True, 'no_new_scope_or_maturity_denominator': True,
                         'source_after': copy.deepcopy(source)}
        self.context = {'schema': BOUNDARY_SCHEMA, 'boundary_id': self.boundary_id,
                        'manifest_path': str(self.directory / 'manifest.json'),
                        'host_supervisor': copy.deepcopy(self.host), 'controller_binding': copy.deepcopy(self.controller),
                        'host_leases': copy.deepcopy(self.host_leases), 'inner_lock_root': str(self.inner_root),
                        'inner_leases': copy.deepcopy(self.inner_leases), 'cleanup': copy.deepcopy(self.cleanup),
                        'host_clearance_claimed': False, 'clearance_scope': CLEARANCE_SCOPE, 'nonDDS': False,
                        'maturity_credit': 'decided_by_unified_runner', 'raw_pid_scope': self.controller['pid_scope'],
                        'raw_network_scope': 'controller boundary; existing nested backend may create its own user/net namespace',
                        'raw_identity_path': str(self.directory / 'raw-identity.json')}
        self.raw_id = {'schema': BOUNDARY_SCHEMA, 'boundary_id': self.boundary_id,
                       'raw_path': self.inner['raw_path'], 'raw_sha256': self.inner['raw_sha256'],
                       'pid_namespace': self.controller['pid_scope'], 'pid_interpretation': PID_INTERPRETATION,
                       'controller_binding': copy.deepcopy(self.controller), 'owned_process_observations': [],
                       'unobserved_worker_mapping': 'unknown; never infer host PID from raw PID equality'}
        self.launcher = {'pid1_reaped': True, 'init_pidfd_exited': True, 'launcher_pid': 1002, 'monotonic_ns': 1900}
        self.final = {'host': copy.deepcopy(self.host), 'guardian_pid': 1001, 'leases_released': True,
                      'leases_quarantined': False, 'monotonic_ns': 3000}
        self.guardian_events = []
        for index, name in enumerate(['host_lease_copies_authenticated', 'owned_launcher_started',
                                      'pid1_kernel_bound_before_gate', 'independent_boundary_authenticated',
                                      'execute_authorized', 'runtime_result_received', 'cleanup_begin',
                                      'cleanup_observed', 'guardian_lease_copies_closing_after_cleanup']):
            self.guardian_events.append({'event': name, 'monotonic_ns': 1000 + index * 10})
        self.guardian_events[0].update(receipts=copy.deepcopy(self.host_leases), owner=copy.deepcopy(self.host), guardian=copy.deepcopy(self.guardian))
        self.guardian_events[1].update(host_pid=1002, start_ticks=1102)
        self.guardian_events[2].update(host_pid=2001, start_ticks=2101)
        self.guardian_events[3]['binding'] = copy.deepcopy(self.controller)
        self.guardian_events[4]['config_sha256'] = digest(inner_config)
        self.guardian_events[6]['error'] = None
        self.guardian_events[7].update(monotonic_ns=2010, receipt=copy.deepcopy(self.cleanup))
        self.guardian_events[8]['monotonic_ns'] = 2100
        self.write()

    def leases(self, root, state, pid, host=False):
        names = ['campaign-' + hashlib.sha256(self.config['campaign_id'].encode()).hexdigest() + '.lock',
                 'domain-181.lock', 'state-' + hashlib.sha256(str(state).encode()).hexdigest() + '.lock']
        rows = []
        for index, name in enumerate(names):
            owner = {'pid': pid, 'campaign_id': self.config['campaign_id'], 'domain_id': 181,
                     'state_dir': str(state), 'acquired_time_ns': 100 + index}
            row = {'path': str(root / name), 'owner': owner, 'acquired': True}
            if host:
                owner.update(start_ticks=self.host['start_ticks'], pid_namespace=self.host['namespaces']['pid'],
                             lease_scope='host_supervisor', boundary_id=self.boundary_id)
                row.update(device=1, inode=100 + index)
            rows.append(row)
        return rows

    def write(self):
        documents = {'request.json': self.request, 'submitted-config.json': self.config,
                     'inner-runtime-result.json': self.inner, 'launcher-cleanup.json': self.launcher,
                     'inner-leases.json': {'pid_scope': self.controller['pid_scope'], 'lock_root': str(self.inner_root),
                                           'receipts': self.inner_leases}}
        for name, value in documents.items():
            write(self.directory / name, value)
        self.manifest['runtime_result_sha256'] = hashlib.sha256((self.directory / 'inner-runtime-result.json').read_bytes()).hexdigest()
        self.manifest['artifact_hashes'] = {name: hashlib.sha256((self.directory / name).read_bytes()).hexdigest() for name in documents}
        self.guardian_events[5]['sha256'] = self.manifest['runtime_result_sha256']
        (self.directory / 'guardian-events.jsonl').write_bytes(b''.join(canonical(row) + b'\n' for row in self.guardian_events))
        write(self.directory / 'manifest.json', self.manifest)
        self.context['manifest_sha256'] = hashlib.sha256((self.directory / 'manifest.json').read_bytes()).hexdigest()
        write(self.directory / 'raw-identity.json', self.raw_id)
        self.context['raw_identity_sha256'] = hashlib.sha256((self.directory / 'raw-identity.json').read_bytes()).hexdigest()
        write(self.directory / 'supervisor-final.json', self.final)
        self.result = copy.deepcopy(self.inner)
        self.result['execution_boundary'] = copy.deepcopy(self.context)
        for key in ('execution', 'execution_metadata'):
            value = self.result.get(key)
            if isinstance(value, dict):
                value.update(pid_namespace=self.controller['pid_scope'], pid_interpretation='namespace_PID_not_host_PID')
                for worker in value.get('workers', []):
                    worker.update(pid_namespace=self.controller['pid_scope'], pid_interpretation='namespace_PID_not_host_PID')
        write(self.root / 'runtime_result.json', self.result)


class BoundaryAuditTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix='r2f-artifact-only-')
        self.addCleanup(self.tmp.cleanup)
        self.f = SyntheticBoundary(Path(self.tmp.name))

    def audit(self):
        return audit_execution_boundary(self.f.config, self.f.result, output_root=self.f.root)

    def reject(self, code=None, field=None):
        value = self.audit()
        self.assertFalse(value['valid'], value)
        self.assertFalse(value['real_execution_attested'])
        self.assertEqual(value['evidence_kind'], 'artifact_consistency_only')
        if code:
            self.assertTrue(any(row['code'] == code for row in value['errors']), value['errors'])
        if field:
            self.assertTrue(any(field in row['field'] for row in value['errors']), value['errors'])
        return value

    def test_synthetic_positive_is_consistency_not_execution(self):
        value = self.audit()
        self.assertTrue(value['valid'], value['errors'])
        self.assertEqual(value['errors'], [])
        self.assertFalse(value['real_execution_attested'])
        self.assertFalse(value['candidate_eligible'])
        self.assertEqual(value['evidence_kind'], 'artifact_consistency_only')
        self.assertTrue(any('sampled' in row for row in value['limitations']))

    def test_no_output_root_uses_standard_directory_without_attesting_it(self):
        value = audit_execution_boundary(self.f.config, self.f.result)
        self.assertTrue(value['valid'], value['errors'])
        self.assertFalse(value['real_execution_attested'])

    def test_read_only_input_objects_and_artifact_bytes(self):
        before = {p: p.read_bytes() for p in self.f.root.rglob('*') if p.is_file()}
        objects = copy.deepcopy((self.f.config, self.f.result))
        with mock.patch('subprocess.Popen', side_effect=AssertionError('no subprocess')):
            self.assertTrue(self.audit()['valid'])
        self.assertEqual(objects, (self.f.config, self.f.result))
        self.assertEqual(before, {p: p.read_bytes() for p in self.f.root.rglob('*') if p.is_file()})

    def test_normalized_hash_not_original_submission_hash(self):
        self.assertNotEqual(digest(self.f.config), self.f.request['submitted_config_sha256'])
        self.assertTrue(self.audit()['valid'])
        self.f.request['submitted_config_sha256'] = digest(self.f.config)
        self.f.write()
        self.reject(field='request.submitted_config_sha256')

    def test_original_config_hash_is_distinct_from_normalized_hash(self):
        self.f.manifest['identity']['config_sha256'] = digest(self.f.normalized)
        self.f.write()
        self.reject(field='manifest.identity.config_sha256')

    def test_inner_lockroot_cannot_change_other_normalized_config(self):
        self.f.request['config']['post_match_count'] += 1
        self.f.request['inner_config_sha256'] = digest(self.f.request['config'])
        self.f.write()
        self.reject(field='request.config.normalized_inner')

    def test_submitted_original_must_equal_caller_config(self):
        self.f.config['domain_id'] = 182
        self.reject(field='submitted-config')

    def test_source_after_must_match_entire_expected_source(self):
        self.f.manifest['source_after']['extra_unbound_field'] = True
        self.f.write()
        self.reject(field='manifest.source_after')

    def test_source_freeze_and_types_are_not_inferred(self):
        self.f.manifest['source_freeze_credit'] = 1
        self.f.write()
        self.reject('unfrozen_source')

    def test_identity_target_unicode_uses_core_canonical_encoding(self):
        self.assertNotEqual(digest(self.f.config['scenario']['target']),
                            digest(self.f.config['scenario']['target'], ensure_ascii=False))
        self.assertTrue(self.audit()['valid'])

    def test_boundary_schema_mismatch(self):
        self.f.request['schema'] = 'invented-schema'
        self.f.write()
        self.reject(field='request.schema')

    def test_boundary_id_mismatch(self):
        self.f.raw_id['boundary_id'] = 'f' * 32
        self.f.write()
        self.reject(field='raw-identity.boundary_id')

    def test_manifest_hash_mismatch(self):
        self.f.result['execution_boundary']['manifest_sha256'] = '0' * 64
        self.reject(field='manifest.sha256')

    def test_raw_identity_hash_mismatch(self):
        self.f.result['execution_boundary']['raw_identity_sha256'] = '0' * 64
        self.reject(field='raw-identity.sha256')

    def test_sealed_request_hash_mismatch(self):
        (self.f.directory / 'request.json').write_text('{}')
        self.reject(field='artifact:request.json.sha256')

    def test_missing_manifest_sealed_launcher_is_contract_gap(self):
        del self.f.manifest['artifact_hashes']['launcher-cleanup.json']
        write(self.f.directory / 'manifest.json', self.f.manifest)
        self.f.result['execution_boundary']['manifest_sha256'] = hashlib.sha256((self.f.directory / 'manifest.json').read_bytes()).hexdigest()
        self.reject('contract_gap', 'launcher-cleanup')

    def test_required_missing_artifact_is_error(self):
        (self.f.directory / 'inner-leases.json').unlink()
        self.reject('unreadable_reference')

    def test_missing_context_fails_closed(self):
        self.f.result.pop('execution_boundary')
        self.reject('contract_gap')

    def test_output_containment_rejects_manifest_from_another_root(self):
        other = self.f.root / 'another-output'
        other.mkdir()
        value = audit_execution_boundary(self.f.config, self.f.result, output_root=other)
        self.assertFalse(value['valid'])
        self.assertTrue(any(row['code'] == 'path_escape' for row in value['errors']))

    def test_symlink_to_external_raw_is_rejected(self):
        with tempfile.TemporaryDirectory() as external:
            path = Path(external) / 'raw.jsonl'
            path.write_bytes(self.f.raw_path.read_bytes())
            self.f.raw_path.unlink()
            self.f.raw_path.symlink_to(path)
            self.reject('path_escape', 'runtime_raw')

    def test_artifact_name_traversal_is_rejected(self):
        self.f.manifest['artifact_hashes']['../escape'] = 'a' * 64
        write(self.f.directory / 'manifest.json', self.f.manifest)
        self.f.result['execution_boundary']['manifest_sha256'] = hashlib.sha256((self.f.directory / 'manifest.json').read_bytes()).hexdigest()
        self.reject('path_escape')

    def test_nonDDS_manifest_cannot_qualify(self):
        self.f.manifest['nonDDS'] = True
        self.f.write()
        self.reject('nonDDS_not_eligible')

    def test_nonDDS_probe_cannot_be_hidden_behind_formal_manifest(self):
        self.f.request['probe'] = {'mode': 'success'}
        self.f.write()
        self.reject('nonDDS_not_eligible', 'request.probe')

    def test_manifest_error_is_not_success(self):
        self.f.manifest['error'] = 'RuntimeError:example'
        self.f.write()
        self.reject('boundary_error')

    def test_host_clearance_is_always_false(self):
        self.f.manifest['host_clearance_claimed'] = True
        self.f.write()
        self.reject('host_clearance_forbidden')

    def test_binding_authentication_must_be_full_existing_declaration(self):
        self.f.manifest['controller_binding']['authentication'] = 'pidfd'
        self.f.write()
        self.reject(field='authentication')

    def test_controller_capability_and_uid_must_be_nonroot(self):
        self.f.manifest['controller_binding']['host']['cap_eff'] = 1
        self.f.manifest['controller_binding']['host']['uid'] = 0
        self.f.write()
        self.reject(field='manifest.controller_binding.host')

    def test_controller_is_inner_pid_one(self):
        self.f.manifest['controller_binding']['inner_pid'] = 2
        self.f.write()
        self.reject(field='inner_pid')

    def test_all_four_namespaces_must_be_separated(self):
        for name in ('user', 'mnt', 'pid', 'net'):
            with self.subTest(namespace=name):
                value = self.f.manifest['controller_binding']['host']['namespaces'][name]
                self.f.manifest['controller_binding']['host']['namespaces'][name] = self.f.host['namespaces'][name]
                self.f.write()
                self.reject('namespace_not_separated')
                self.f.manifest['controller_binding']['host']['namespaces'][name] = value

    def test_uid_map_wrong_range(self):
        self.f.manifest['controller_binding']['host']['uid_map'] = '0 1000 1\n'
        self.f.write()
        self.reject(field='uid_map')

    def test_gid_map_multiuser_is_rejected(self):
        self.f.manifest['controller_binding']['host']['gid_map'] = '1000 1000 2\n'
        self.f.write()
        self.reject('invalid_gid_map')

    def test_raw_pid_cannot_be_reinterpreted_as_host_pid(self):
        self.f.raw_id['pid_interpretation'] = 'host PID'
        self.f.write()
        self.reject(field='raw-identity.pid_interpretation')

    def test_raw_scope_must_match_authenticated_controller(self):
        self.f.context['raw_pid_scope'] = self.f.host['namespaces']['pid']
        self.f.write()
        self.reject(field='context.raw_pid_scope')

    def test_raw_hash_is_bound_to_file_bytes(self):
        self.f.raw_path.write_bytes(self.f.raw_path.read_bytes() + b'{}\n')
        self.reject(field='runtime_raw.sha256')

    def test_manifest_runtime_hash_is_original_inner_result(self):
        self.f.manifest['runtime_result_sha256'] = '0' * 64
        write(self.f.directory / 'manifest.json', self.f.manifest)
        self.f.result['execution_boundary']['manifest_sha256'] = hashlib.sha256((self.f.directory / 'manifest.json').read_bytes()).hexdigest()
        self.reject(field='manifest.runtime_result_sha256')

    def test_wrapper_cannot_change_sut_facts(self):
        self.f.result['facts']['only'] = 'changed by wrapper'
        self.reject(field='wrapper_result_projection')

    def test_wrapper_cannot_change_worker_or_metadata_except_annotations(self):
        self.f.result['execution_metadata']['workers'][0]['pid'] = 999
        self.reject(field='wrapper_result_projection')

    def test_wrapper_annotation_value_must_be_namespace_not_host(self):
        self.f.result['execution']['workers'][0]['pid_interpretation'] = 'host PID'
        self.reject(field='wrapper_result_projection')

    def test_lease_count_cannot_shrink(self):
        self.f.manifest['inner_leases'].pop()
        self.f.write()
        self.reject('lease_count')

    def test_host_lease_start_and_inode_are_required(self):
        self.f.manifest['host_leases'][0].pop('inode')
        self.f.manifest['host_leases'][0]['owner']['start_ticks'] += 1
        self.f.write()
        self.reject(field='host_leases[0]')

    def test_inner_lease_pid_is_namespace_pid_one_not_host_pid(self):
        self.f.manifest['inner_leases'][0]['owner']['pid'] = 2001
        self.f.write()
        self.reject(field='inner_leases[0].owner.pid')

    def test_raw_lease_event_cannot_be_replaced_by_receipt_only(self):
        self.f.inner['events'] = []
        self.f.write()
        self.reject(field='inner.events.raw')

    def test_cleanup_must_confirm_pid1_launcher_and_all_listed_descendants(self):
        for key in ('pid1_exited', 'launcher_exited', 'confirmed'):
            with self.subTest(field=key):
                self.f.cleanup[key] = False
                self.f.write()
                self.reject('cleanup_unconfirmed')
                self.f.cleanup[key] = True
        self.f.cleanup['descendants'][0]['pidfd_exited'] = False
        self.f.write()
        self.reject('cleanup_unconfirmed')

    def test_sampled_process_list_is_not_complete_exit_proof(self):
        self.f.cleanup.pop('method')
        self.f.manifest['owned_process_observations'] = [{'pid': 2002, 'sampled': True, 'exited': True}]
        self.f.write()
        self.reject(field='manifest.cleanup.method')

    def test_launcher_reap_must_be_sealed_and_positive(self):
        self.f.launcher['pid1_reaped'] = False
        self.f.write()
        self.reject('contract_gap', 'launcher-cleanup.pid1_reaped')

    def test_leases_released_and_not_quarantined(self):
        self.f.final['leases_quarantined'] = True
        self.f.write()
        self.reject('leases_not_released')

    def test_host_release_cannot_precede_namespace_cleanup(self):
        self.f.final['monotonic_ns'] = 100
        self.f.write()
        self.reject('cleanup_chronology')

    def test_unsealed_guardian_event_cannot_supply_missing_manifest_binding(self):
        self.f.manifest.pop('controller_binding')
        self.f.write()
        self.reject('contract_gap', 'manifest.controller_binding')

    def test_auxiliary_guardian_events_remain_auxiliary_even_when_consistent(self):
        self.assertNotIn('guardian-events.jsonl', self.f.manifest['artifact_hashes'])
        value = self.audit()
        self.assertTrue(value['valid'], value['errors'])
        self.assertFalse(value['real_execution_attested'])
        self.assertTrue(any('Guardian JSONL' in row for row in value['limitations']))

    def test_guardian_lease_reference_mismatch_is_error_not_live_proof(self):
        self.f.guardian_events[0]['receipts'] = []
        self.f.write()
        self.reject(field='guardian_events_auxiliary.host_lease')

    def test_guardian_host_owner_cannot_disagree_with_manifest(self):
        self.f.guardian_events[0]['owner']['start_ticks'] += 1
        self.f.write()
        self.reject(field='guardian_events_auxiliary.host_lease_copies_authenticated.owner')

    def test_guardian_pid1_start_ticks_are_bound_to_controller(self):
        self.f.guardian_events[2]['start_ticks'] += 1
        self.f.write()
        self.reject(field='guardian_events_auxiliary.pid1_kernel_bound_before_gate.start_ticks')

    def test_reference_changed_during_audit_is_error(self):
        original = Path.read_bytes
        calls = 0
        def changing(path):
            nonlocal calls
            data = original(path)
            if path == self.f.raw_path:
                calls += 1
                if calls >= 2:
                    return data + b'changed after first read'
            return data
        with mock.patch.object(Path, 'read_bytes', changing):
            self.reject('artifact_changed')

    def test_guardian_chronology_reordered_is_error(self):
        self.f.guardian_events[2]['monotonic_ns'] = 1
        self.f.write()
        self.reject('guardian_chronology')

    def test_duplicate_json_keys_are_not_silently_accepted(self):
        (self.f.directory / 'request.json').write_text('{"probe":null,"probe":{}}')
        self.reject('invalid_artifact', 'request.json')

    def test_missing_descendants_array_is_contract_gap_not_vacuous_success(self):
        self.f.cleanup.pop('descendants')
        self.f.write()
        self.reject('contract_gap', 'manifest.cleanup.descendants')

    def test_cleanup_descendant_namespace_must_be_owned_by_controller(self):
        self.f.cleanup['descendants'][0]['binding']['host']['namespaces']['pid'] = 'pid:[999]'
        self.f.write()
        self.reject(field='controller_namespace.pid')

    def test_guardian_cannot_gain_seal_credit_from_invented_hash_scope(self):
        path = self.f.directory / 'guardian-events.jsonl'
        self.f.manifest['artifact_hashes']['guardian-events.jsonl'] = hashlib.sha256(path.read_bytes()).hexdigest()
        write(self.f.directory / 'manifest.json', self.f.manifest)
        self.f.result['execution_boundary']['manifest_sha256'] = hashlib.sha256((self.f.directory / 'manifest.json').read_bytes()).hexdigest()
        self.reject('guardian_hash_scope_mismatch')

    def test_persisted_outer_result_must_equal_supplied_result(self):
        value = copy.deepcopy(self.f.result)
        value['facts'] = {'unbound': 'result'}
        write(self.f.root / 'runtime_result.json', value)
        self.reject(field='wrapper-runtime-result.argument')

    def test_raw_network_scope_is_not_host_clearance(self):
        self.f.context['raw_network_scope'] = 'host network cleared'
        self.f.write()
        self.reject(field='context.raw_network_scope')

    def test_incomplete_host_to_inner_pid_chain_is_contract_gap(self):
        self.f.manifest['controller_binding']['host']['nspid'] = [2001]
        self.f.write()
        self.reject('contract_gap', 'host.nspid')

    def test_missing_nonce_reference_digest_is_contract_gap(self):
        self.f.result['execution_boundary'].pop('raw_identity_sha256')
        self.reject('contract_gap', 'raw-identity.sha256')

    def test_missing_supervisor_final_cannot_be_replaced_by_guardian_log(self):
        (self.f.directory / 'supervisor-final.json').unlink()
        self.reject('unreadable_reference', 'supervisor-final')

    def test_actual_raw_lease_owner_cannot_disagree_with_recorded_inner_leases(self):
        self.f.raw[0]['locks'][0]['owner']['pid'] = 2001
        self.f.raw_path.write_bytes(b''.join(canonical(row) + b'\n' for row in self.f.raw))
        self.f.inner['events'] = copy.deepcopy(self.f.raw)
        self.f.inner['raw_sha256'] = hashlib.sha256(self.f.raw_path.read_bytes()).hexdigest()
        self.f.raw_id['raw_sha256'] = self.f.inner['raw_sha256']
        self.f.write()
        self.reject(field='runtime_raw.inner_leases')

    def test_empty_cleanup_descendant_list_is_not_claim_of_complete_scan(self):
        # This is the current formal writer's shape: non-fixture descendants
        # are covered by declared PIDNS teardown, not a fabricated pidfd list.
        self.f.cleanup['descendants'] = []
        self.f.context['cleanup'] = copy.deepcopy(self.f.cleanup)
        self.f.guardian_events[7]['receipt'] = copy.deepcopy(self.f.cleanup)
        self.f.write()
        value = self.audit()
        self.assertTrue(value['valid'], value['errors'])
        self.assertFalse(value['real_execution_attested'])
        self.assertTrue(any('sampled' in row for row in value['limitations']))

    def test_malformed_inputs_return_concrete_errors_without_executing(self):
        for config, result in ((None, None), ({}, []), (self.f.config, {'execution_boundary': None})):
            with self.subTest(config=config):
                value = audit_execution_boundary(config, result)
                self.assertFalse(value['valid'])
                self.assertTrue(value['errors'])
                self.assertFalse(value['real_execution_attested'])

    def test_unreadable_artifact_is_error(self):
        original = Path.read_bytes
        def denied(path):
            if path == self.f.directory / 'inner-runtime-result.json':
                raise PermissionError('offline simulated read denial')
            return original(path)
        with mock.patch.object(Path, 'read_bytes', denied):
            self.reject('invalid_artifact')

    def test_existing_nonDDS_artifacts_are_rejected_read_only(self):
        output = ROOT / 'results/main-review/execution-boundary-20261117/deep-success-v2'
        result_path = output / 'runtime_result.json'
        if not result_path.exists():
            self.skipTest('historical nonDDS artifacts absent; synthetic refusal still tested')
        directory = next(output.glob('execution-boundary-*'))
        config = json.loads((directory / 'submitted-config.json').read_text())
        result = json.loads(result_path.read_text())
        before = {p: hashlib.sha256(p.read_bytes()).hexdigest() for p in directory.iterdir() if p.is_file()}
        value = audit_execution_boundary(config, result, output_root=output)
        self.assertFalse(value['valid'])
        self.assertFalse(value['real_execution_attested'])
        self.assertTrue(any(row['code'] == 'nonDDS_not_eligible' for row in value['errors']), value['errors'])
        self.assertEqual(before, {p: hashlib.sha256(p.read_bytes()).hexdigest() for p in directory.iterdir() if p.is_file()})


if __name__ == '__main__':
    unittest.main()
