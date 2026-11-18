"""Synthetic file-only nested bridge fixtures, never ROS/kernel evidence."""
from copy import deepcopy
import hashlib
import json
import sys
from pathlib import Path

import pytest

from r2f.campaign.boundary_audit import (audit_execution_boundary, AUTHENTICATION,
    TOPOLOGY_NAMESPACE_SCHEMA, TOPOLOGY_MANIFEST_SCHEMA, NESTED_CLEANUP_METHOD)
from r2f.core import canonical_hash
from r2f.runtime.ros2_pair import normalize_config
from test_execution_boundary_audit import SyntheticBoundary, canonical, digest, write


def file_ref(path, kind='json'):
    return {'path': str(path), 'sha256': hashlib.sha256(path.read_bytes()).hexdigest(), 'kind': kind}


class SyntheticNested(SyntheticBoundary):
    """Deliberately fabricated schema-only artifacts. Zero live attestation."""
    def __init__(self, root):
        super().__init__(root)
        self.child_root = root / 'inner-runtime'
        self.side = self.child_root / 'topology-namespace'
        self.side.mkdir()
        from test_topology_replay import _context_input
        _, config = deepcopy(_context_input())
        original = self.config
        config.update({k: deepcopy(original[k]) for k in ('run_id', 'campaign_id', 'domain_id',
                       'source_manifest', 'source_repo', 'expected_head', 'lock_root', 'execution_boundary')})
        # Author the synthetic source identity through the actual core Scenario
        # from the registry/provider capsule; never patch a sealed input dict.
        from r2f.core.scenario import Scenario
        scenario = Scenario.from_dict(config['scenario'])
        input_args = dict(final_input=config['final_input'],
                          input_layer_receipts=config['input_layer_receipts'],
                          source_manifest=config['source_manifest'])
        config['effective_input'] = scenario.effective_input_v2(**input_args)
        config['effective_input_sha256'] = scenario.input_hash_v2(**input_args)
        config['live_execution_go'] = True  # synthetic execute-intent declaration, NOT evidence
        self.config = config
        self.normalized = normalize_config(deepcopy(config))
        self.inner_config = dict(self.normalized, lock_root=str(self.inner_root))
        self.request.update(config=deepcopy(self.inner_config), submitted_config_sha256=digest(self.normalized),
                            inner_config_sha256=digest(self.inner_config))
        identity = self.manifest['identity']
        identity.update(config_sha256=digest(config), source_manifest=deepcopy(config['source_manifest']),
                        target_sha256=digest(config['scenario']['target'], ensure_ascii=False),
                        effective_input_sha256=config['effective_input_sha256'])
        self.request['identity'] = deepcopy(identity)
        self.guardian_events[4]['config_sha256'] = digest(self.inner_config)
        outer = deepcopy(self.controller['host'])
        outer.update(pid=1, nspid=[1])
        nested = deepcopy(outer)
        nested.update(pid=1, nspid=[1], uid=0, cap_eff=(1 << 12), start_ticks=2200,
                      uid_map='0 1000 1\n', gid_map='0 1000 1\n',
                      namespaces={k: k + ':[' + str(201 + i) + ']' for i, k in enumerate(('user', 'mnt', 'pid', 'net'))})
        ancestor = deepcopy(nested); ancestor.update(pid=3, nspid=[3, 1], uid=1000)
        self.nested_binding = {'authentication': AUTHENTICATION, 'outer': ancestor, 'reported_inner': nested,
                               'inner_pid': 1, 'parent_pid_scope': self.controller['pid_scope'],
                               'pid_scope': nested['namespaces']['pid'], 'pid_namespace_chain': [
                                   {'pid': 3, 'pid_scope': self.controller['pid_scope']},
                                   {'pid': 1, 'pid_scope': nested['namespaces']['pid']} ]}
        self.ns_manifest = {'schema': TOPOLOGY_MANIFEST_SCHEMA, 'bridge_id': 'b' * 32,
            'config_sha256': digest(self.inner_config), 'source_manifest': deepcopy(config['source_manifest']),
            'outer_controller': outer, 'nested_controller_binding': deepcopy(self.nested_binding),
            'raw_pid_scope': self.nested_binding['pid_scope'], 'output_root': str(self.child_root),
            'error': None, 'host_clearance_claimed': False, 'maturity_credit': False,
            'parent_process_observations': [], 'cleanup': {'confirmed': True, 'controller_exited': True,
                'wrapper_returncode': 0, 'method': NESTED_CLEANUP_METHOD}}
        self.child_request = {'config': deepcopy(self.inner_config), 'config_sha256': digest(self.inner_config),
            'source_manifest': deepcopy(config['source_manifest']), 'outer': outer,
            'options': deepcopy(self.request['options']), 'output_root': str(self.child_root),
            'bridge_id': self.ns_manifest['bridge_id']}
        self.raw = []
        def emit(event, **fields):
            row = {'event': event, 'event_index': len(self.raw), 'run_id': config['run_id'],
                   'domain_id': 181, 'pid': 1, 'pgid': 1, 'monotonic_ns': 100 + len(self.raw), **fields}
            self.raw.append(row)
            return row
        emit('isolation_locks_acquired', locks=deepcopy(self.inner_leases))
        self.lease_scope = {'controller_start_ticks': nested['start_ticks'], 'owner_pid': 1,
            'pid_scope': nested['namespaces']['pid'], 'lock_root': str(self.inner_root),
            'state_dir': str(self.child_root), 'receipts': deepcopy(self.inner_leases)}
        network = {'schema_version': 1, 'host_uid': 1000, 'host_cap_eff': 0, 'launcher_pid': 1,
            'host_net_namespace': outer['namespaces']['net'], 'host_user_namespace': outer['namespaces']['user'],
            'net_namespace': nested['namespaces']['net'], 'user_namespace': nested['namespaces']['user'],
            'observed_net_namespace': nested['namespaces']['net'], 'observed_user_namespace': nested['namespaces']['user'],
            'cap_eff': nested['cap_eff'], 'uid_map': [[0, 1000, 1]], 'euid': 0,
            'isolated': True, 'interfaces': ['lo'], 'loopback_flags': 9}
        self.proof = {'identity': deepcopy(nested), 'network': network, 'pid1_proc_scoped': True,
            'active_proc_mount_id': '99', 'proc_mounts': [{'mount': '/proc', 'root': '/', 'fs_type': 'proc', 'mount_id': '99'}]}
        ready = emit('topology_namespace_ready', namespace=deepcopy(self.proof), lease_scope=deepcopy(self.lease_scope),
                     config_sha256=digest(self.inner_config), execution_evidence=True)
        self.lease_scope['raw_event_ref'] = ready['event_index']
        self.env = {'ok': True, 'environment_mode': 'ros_overlay', 'ros_distro': 'jazzy',
            'domain_id': 181, 'target_revision': config['target_revision'],
            'source': {k: config['source_manifest'][k] for k in ('head', 'source_repo')},
            'rmw': {config['rmw']: {'available': True}}, 'dependency_lock_sha256': 'f' * 64,
            'process_scan': {'ok': True}, 'old_lock_scan': {'ok': True}}
        emit('environment_preflight', ok=True, environment=deepcopy(self.env))
        self.workers, self.worker_bindings = [], []
        for number, role in enumerate(('writer', 'reader')):
            token = {'logical_id': role + '_synthetic', 'generation': 1, 'worker_generation': 1,
                'pid': 9 + number, 'pgid': 9 + number, 'process_start_ticks': 2300 + number,
                'endpoint_gid': ('a' if number else 'b') * 32, 'endpoint_generation': role + '-synthetic-epoch',
                'role': role, 'worker_role': 'topology_' + role, 'real_ros': True, 'backend': 'ros2_public_api',
                'observed_domain_id': 181, 'domain_observation_source': 'worker_environment', 'rmw_identifier': config['rmw']}
            def worker_event(kind, **fields):
                row = emit(kind, **dict(token, **fields))
                return {'raw_path': str(self.raw_path), 'event_index': row['event_index']}
            start = worker_event('process_started', fresh_process=True, separate_context=True)
            created = worker_event('endpoint_created')
            bound = {k: token[k] for k in ('logical_id', 'generation', 'pid', 'process_start_ticks', 'endpoint_gid', 'endpoint_generation')}
            bound.update(namespaces=deepcopy(nested['namespaces']), host_pid=None, outer_pid=None,
                mapping_status='nested_kernel_bound; ancestor mapping unobserved unless independently sampled')
            row = emit('topology_namespace_worker_bound', binding=deepcopy(bound), execution_evidence=True)
            bound['raw_event_ref'] = row['event_index']; self.worker_bindings.append(bound)
            exited = worker_event('process_exit', returncode=0)
            cleanup = worker_event('endpoint_destroyed', cleanup_status='closed')
            worker = {k: deepcopy(token[k]) for k in ('logical_id', 'generation', 'pid', 'pgid',
                       'process_start_ticks', 'endpoint_gid', 'endpoint_generation', 'worker_generation', 'rmw_identifier')}
            worker.update(role='topology_' + role, endpoint_role=role, fixture=False, real_ros=True,
                domain_id=181, rmw=config['rmw'], execution_domain_id=181, domain_observation_source='worker_environment',
                fresh_process=True, separate_context=True, closed=True, returncode=0, cleanup_status='closed', unknowns=[],
                token_raw_ref=created, start_raw_refs=[start], exit_raw_refs=[exited], cleanup_raw_refs=[cleanup])
            self.workers.append(worker)
        metadata = {k: deepcopy(config.get(k)) for k in ('run_id', 'target_id', 'target_revision', 'rmw',
            'arm', 'repeat', 'domain_id', 'seed', 'mutation_kind', 'carrier', 'effective_input_sha256', 'source_manifest')}
        metadata.update(workers=deepcopy(self.workers), declared_domain_id=181, declared_rmw=config['rmw'],
            source_identity=deepcopy(self.env['source']), source_after=deepcopy(config['source_manifest']),
            context_basis='sealed_input_declaration_not_sut_observation',
            input_program_sha256=canonical_hash(config['final_input']['topology_program']),
            sealed_execution_context={k: deepcopy(config.get(k)) for k in ('run_id', 'target_id', 'target_revision',
                'rmw', 'domain_id', 'arm', 'repeat', 'input_identity_schema', 'effective_input_sha256', 'plan_hash')})
        write(self.child_root / 'environment.json', self.env)
        metadata['environment_manifest'] = {k: v for k, v in file_ref(self.child_root / 'environment.json').items() if k != 'kind'}
        emit('topology_execution_metadata', metadata=deepcopy(metadata))
        receipt = {'closed': True, 'survivors': [], 'errors': []}
        emit('topology_cleanup', receipt=receipt)
        emit('runtime_end')
        self.child = {'events': deepcopy(self.raw), 'environment': deepcopy(self.env), 'execution_metadata': metadata,
            'execution': {'workers': deepcopy(self.workers)}, 'evidence_origin': 'fixture', 'real_execution': False,
            'status': 'executed', 'runtime_status': 'completed', 'cleanup_receipt': receipt,
            'raw_path': str(self.raw_path)}
        self.sync()

    def sync(self):
        """Re-seal synthetic artifacts after a chosen semantic alteration."""
        self.child['events'] = deepcopy(self.raw)
        self.raw_path.write_bytes(b''.join(canonical(e) + b'\n' for e in self.raw))
        self.child['raw_sha256'] = file_ref(self.raw_path)['sha256']
        write(self.child_root / 'execution_metadata.json', self.child['execution_metadata'])
        write(self.child_root / 'runtime_result.json', self.child)
        documents = {'request_ref': ('request.json', self.child_request),
            'inner_result_ref': ('child-runtime-result.json', self.child),
            'source_before_ref': ('source-before.json', self.config['source_manifest']),
            'source_after_ref': ('source-after.json', self.config['source_manifest'])}
        for key, (name, value) in documents.items():
            write(self.side / name, value); self.ns_manifest[key] = file_ref(self.side / name)
        caller = {key: self.ns_manifest['outer_controller']['namespaces'][name]
                  for key, name in [('host_net_namespace', 'net'), ('host_user_namespace', 'user')]}
        caller.update(host_uid=1000, host_cap_eff=0, launcher_pid=1)
        self.ns_manifest['command'] = ['unshare', '--user', '--map-root-user', '--net', '--pid', '--fork',
            '--mount', '--mount-proc', '--kill-child=SIGKILL', '--', sys.executable, '-B', '-m',
            'r2f.runtime.discovery_network', '_enter', json.dumps(caller, sort_keys=True), '--',
            sys.executable, '-B', '-m', 'r2f.runtime.topology_namespace', '_child',
            str(self.side / 'request.json'), self.ns_manifest['request_ref']['sha256'], '10']
        self.ns_manifest['raw_ref'] = file_ref(self.raw_path, 'jsonl')
        def full_ref(index):
            return {**{k: self.ns_manifest['raw_ref'][k] for k in ('path', 'sha256')}, 'event_index': index}
        leases = deepcopy(self.lease_scope)
        leases['raw_event_ref'] = full_ref(leases['raw_event_ref'])
        workers = [dict(deepcopy(w), raw_event_ref=full_ref(w['raw_event_ref'])) for w in self.worker_bindings]
        self.ns_manifest.update(inner_lease_scope=leases, worker_bindings=workers)
        envelope = {k: deepcopy(self.ns_manifest[k]) for k in ('inner_result_ref', 'source_before_ref', 'source_after_ref', 'raw_ref')}
        envelope.update(inner_lease_scope=deepcopy(self.lease_scope), worker_bindings=deepcopy(self.worker_bindings))
        write(self.side / 'child-envelope.json', envelope)
        self.ns_manifest['envelope_ref'] = file_ref(self.side / 'child-envelope.json')
        write(self.side / 'manifest.json', self.ns_manifest)
        desc = {k: deepcopy(self.ns_manifest[k]) for k in ('bridge_id', 'outer_controller', 'nested_controller_binding',
            'raw_pid_scope', 'inner_lease_scope', 'worker_bindings', 'cleanup')}
        desc.update(schema=TOPOLOGY_NAMESPACE_SCHEMA, manifest_path=str(self.side / 'manifest.json'),
                    manifest_sha256=file_ref(self.side / 'manifest.json')['sha256'])
        self.inner = dict(deepcopy(self.child), topology_namespace=desc)
        self.seal_outer()

    def seal_outer(self):
        """Seal only the enclosing synthetic boundary, never rewrite child artifacts."""
        desc = self.inner['topology_namespace']
        self.raw_id.update(raw_sha256=self.child['raw_sha256'], topology_namespace=deepcopy(desc),
            nested_controller_binding=deepcopy(self.nested_binding), pid_namespace=self.nested_binding['pid_scope'])
        self.context.update(topology_namespace=deepcopy(desc), nested_controller_binding=deepcopy(self.nested_binding),
            raw_pid_scope=self.nested_binding['pid_scope'], raw_network_scope='authenticated nested topology controller and workers')
        # Super writes exact legacy envelope first; the two new lease fields are
        # then sealed into the same manifest without touching legacy fixtures.
        self.write()
        lease_path = self.directory / 'inner-leases.json'
        value = json.loads(lease_path.read_text())
        value.update(pid_scope=self.nested_binding['pid_scope'], outer_pid_scope=self.controller['pid_scope'],
                     nested_controller_binding=deepcopy(self.nested_binding))
        write(lease_path, value)
        self.manifest['artifact_hashes']['inner-leases.json'] = file_ref(lease_path)['sha256']
        write(self.directory / 'manifest.json', self.manifest)
        self.context['manifest_sha256'] = file_ref(self.directory / 'manifest.json')['sha256']
        self.result = deepcopy(self.inner)
        self.result['execution_boundary'] = deepcopy(self.context)
        for key in ('execution', 'execution_metadata'):
            value = self.result.get(key)
            if isinstance(value, dict):
                for item in [value, *value.get('workers', [])]:
                    item.update(pid_namespace=self.nested_binding['pid_scope'], pid_interpretation='namespace_PID_not_host_PID')
        write(self.root / 'runtime_result.json', self.result)

    def audit(self):
        return audit_execution_boundary(self.config, self.result, output_root=self.root)


@pytest.fixture
def nested(tmp_path):
    return SyntheticNested(tmp_path)


def test_nested_synthetic_consistency_produces_only_audited_inner_view(nested):
    audit = nested.audit()
    assert audit['valid'], audit['errors']
    assert audit['inner_artifact_view']['output_root'] == str(nested.child_root)
    assert audit['inner_artifact_view']['raw_pid_scope'] != nested.controller['pid_scope']
    assert not audit['real_execution_attested'] and not audit['candidate_eligible']
    assert nested.child['evidence_origin'] == 'fixture'
    assert nested.result['execution_metadata']['workers'][0]['pid_namespace'] == nested.nested_binding['pid_scope']
    assert nested.ns_manifest['worker_bindings'][0]['host_pid'] is None


@pytest.mark.parametrize('damage', ['rawscope', 'outer_scope', 'uid_map', 'gid_map', 'pid1_collision',
    'worker_pid', 'worker_generation', 'worker_gid', 'worker_ticks', 'worker_namespace', 'host_mapping',
    'source', 'config', 'raw_hash', 'result_hash', 'cleanup', 'leaseholder', 'lease_scope',
    'fake_proc', 'ready_identity', 'ready_hash', 'chronology', 'envelope_ref_type', 'missing_manifest'])
def test_nested_semantic_tamper_fails_closed_after_rehash(nested, damage):
    f = nested
    if damage == 'rawscope': f.ns_manifest['raw_pid_scope'] = f.controller['pid_scope']
    elif damage == 'outer_scope': f.nested_binding['parent_pid_scope'] = f.nested_binding['pid_scope']
    elif damage in ('uid_map', 'gid_map'): f.nested_binding['reported_inner'][damage] = '0 0 4294967295\n'
    elif damage == 'pid1_collision': f.nested_binding['outer'].update(pid=1, nspid=[1])
    elif damage.startswith('worker_') or damage == 'host_mapping':
        worker = f.worker_bindings[0]
        if damage == 'worker_pid': worker['pid'] += 20
        elif damage == 'worker_generation': worker['generation'] += 1
        elif damage == 'worker_gid': worker['endpoint_gid'] = 'foreign'
        elif damage == 'worker_ticks': worker['process_start_ticks'] += 1
        elif damage == 'worker_namespace': worker['namespaces']['pid'] = f.controller['pid_scope']
        else: worker['host_pid'] = worker['pid']
    elif damage == 'source': f.ns_manifest['source_manifest']['head'] = 'c' * 40
    elif damage == 'config': f.child_request['config']['domain_id'] = 99
    elif damage == 'cleanup': f.ns_manifest['cleanup']['confirmed'] = False
    elif damage == 'leaseholder': f.lease_scope['receipts'][0]['owner']['pid'] = 3
    elif damage == 'lease_scope': f.lease_scope['pid_scope'] = f.controller['pid_scope']
    elif damage == 'fake_proc': f.raw[1]['namespace']['active_proc_mount_id'] = 'unobserved'
    elif damage == 'ready_identity': f.raw[1]['namespace']['identity']['start_ticks'] += 1
    elif damage == 'ready_hash': f.raw[1]['config_sha256'] = '0' * 64
    elif damage == 'chronology': f.raw[-1]['monotonic_ns'] = 1
    f.ns_manifest['nested_controller_binding'] = deepcopy(f.nested_binding)
    f.sync()
    if damage in ('raw_hash', 'result_hash', 'envelope_ref_type'):
        if damage == 'raw_hash': f.ns_manifest['raw_ref']['sha256'] = '0' * 64
        elif damage == 'result_hash': f.ns_manifest['inner_result_ref']['sha256'] = '0' * 64
        else:
            path = f.side / 'child-envelope.json'; data = json.loads(path.read_text())
            data['inner_lease_scope']['raw_event_ref'] = deepcopy(f.ns_manifest['inner_lease_scope']['raw_event_ref'])
            write(path, data); f.ns_manifest['envelope_ref'] = file_ref(path)
        reseal_manifest(f)
    if damage == 'missing_manifest': (f.side / 'manifest.json').unlink()
    audit = f.audit()
    assert not audit['valid'], damage
    assert 'inner_artifact_view' not in audit
    assert not audit['candidate_eligible']


def reseal_manifest(f):
    """Rehash descriptor and wrapper, without laundering semantic mismatches."""
    write(f.side / 'manifest.json', f.ns_manifest)
    desc = f.inner['topology_namespace']
    desc['manifest_sha256'] = file_ref(f.side / 'manifest.json')['sha256']
    f.context['topology_namespace'] = deepcopy(desc)
    f.raw_id['topology_namespace'] = deepcopy(desc)
    # sync would overwrite intentionally damaged sidecars, so only outer seal.
    write(f.directory / 'inner-runtime-result.json', f.inner)
    f.manifest['runtime_result_sha256'] = file_ref(f.directory / 'inner-runtime-result.json')['sha256']
    f.manifest['artifact_hashes']['inner-runtime-result.json'] = f.manifest['runtime_result_sha256']
    f.guardian_events[5]['sha256'] = f.manifest['runtime_result_sha256']
    (f.directory / 'guardian-events.jsonl').write_bytes(b''.join(canonical(e) + b'\n' for e in f.guardian_events))
    write(f.directory / 'manifest.json', f.manifest)
    f.context['manifest_sha256'] = file_ref(f.directory / 'manifest.json')['sha256']
    write(f.directory / 'raw-identity.json', f.raw_id)
    f.context['raw_identity_sha256'] = file_ref(f.directory / 'raw-identity.json')['sha256']
    result = deepcopy(f.inner); result['execution_boundary'] = deepcopy(f.context)
    for key in ('execution', 'execution_metadata'):
        for item in [result[key], *result[key].get('workers', [])]:
            item.update(pid_namespace=f.nested_binding['pid_scope'], pid_interpretation='namespace_PID_not_host_PID')
    f.result = result; write(f.root / 'runtime_result.json', result)


@pytest.mark.parametrize('damage', ['raw_identity_scope', 'context_scope', 'network_scope', 'outer_binding',
    'inner_leases_scope', 'manifest_unknown', 'descriptor_flags', 'binding_host_label', 'request_bridge',
    'request_source', 'envelope_worker_ref', 'worker_ref_bool', 'worker_raw_binding', 'duplicate_worker',
    'network_uid', 'proc_root', 'source_after', 'command_hash', 'command_flags', 'command_caller'])
def test_nested_cross_surface_forgery_rejected(nested, damage):
    f = nested
    if damage == 'raw_identity_scope': f.raw_id['pid_namespace'] = f.controller['pid_scope']; f.sync()
    elif damage == 'context_scope': f.context['raw_pid_scope'] = f.controller['pid_scope']; f.sync()
    elif damage == 'network_scope':
        f.context['raw_network_scope'] = 'controller boundary; existing nested backend may create its own user/net namespace'; f.sync()
    elif damage == 'outer_binding':
        f.context['controller_binding']['host']['start_ticks'] += 1; f.sync()
    elif damage == 'manifest_unknown': f.ns_manifest['kernel_verified'] = True; f.sync()
    elif damage == 'binding_host_label':
        f.nested_binding['host'] = deepcopy(f.nested_binding['outer'])
        f.ns_manifest['nested_controller_binding'] = deepcopy(f.nested_binding); f.sync()
    elif damage == 'request_bridge': f.child_request['bridge_id'] = 'c' * 32; f.sync()
    elif damage == 'request_source': f.child_request['source_manifest']['head'] = 'c' * 40; f.sync()
    elif damage == 'worker_raw_binding':
        idx = f.worker_bindings[0]['raw_event_ref']; f.raw[idx]['binding']['process_start_ticks'] += 1; f.sync()
    elif damage == 'duplicate_worker': f.worker_bindings.append(deepcopy(f.worker_bindings[0])); f.sync()
    elif damage == 'network_uid': f.raw[1]['namespace']['network']['host_uid'] = 0; f.sync()
    elif damage == 'proc_root': f.raw[1]['namespace']['proc_mounts'][0]['root'] = '/foreign'; f.sync()
    elif damage == 'source_after':
        write(f.side / 'source-after.json', dict(f.config['source_manifest'], head='d' * 40))
        f.ns_manifest['source_after_ref'] = file_ref(f.side / 'source-after.json'); reseal_manifest(f)
    elif damage.startswith('command_'):
        if damage == 'command_hash': f.ns_manifest['command'][23] = '0' * 64
        elif damage == 'command_flags': f.ns_manifest['command'][4] = '--no-pid'
        else: f.ns_manifest['command'][15] = '{}'
        reseal_manifest(f)
    elif damage == 'descriptor_flags':
        f.inner['topology_namespace']['root_verified'] = True; reseal_manifest(f)
    elif damage == 'envelope_worker_ref':
        path = f.side / 'child-envelope.json'; value = json.loads(path.read_text())
        value['worker_bindings'][0]['raw_event_ref'] = deepcopy(f.ns_manifest['worker_bindings'][0]['raw_event_ref'])
        write(path, value); f.ns_manifest['envelope_ref'] = file_ref(path); reseal_manifest(f)
    elif damage == 'worker_ref_bool':
        f.ns_manifest['worker_bindings'][0]['raw_event_ref']['event_index'] = True; reseal_manifest(f)
    else:
        path = f.directory / 'inner-leases.json'; value = json.loads(path.read_text())
        value['pid_scope'] = f.controller['pid_scope']; write(path, value)
        f.manifest['artifact_hashes']['inner-leases.json'] = file_ref(path)['sha256']
        reseal_manifest(f)
    # sync refreshes legitimate annotations by design; change them only after
    # outer sealing when testing a mismatched cross-surface annotation.
    if damage in ('raw_identity_scope', 'context_scope', 'network_scope'):
        if damage == 'raw_identity_scope': f.raw_id['pid_namespace'] = f.controller['pid_scope']
        elif damage == 'context_scope': f.context['raw_pid_scope'] = f.controller['pid_scope']
        else: f.context['raw_network_scope'] = 'unproven root flags'
        reseal_manifest(f)
    audit = f.audit()
    assert not audit['valid'], damage
    assert 'inner_artifact_view' not in audit


def test_nested_raw_bytes_and_legacy_output_are_immutable(nested):
    before = nested.raw_path.read_bytes()
    first = nested.audit(); second = nested.audit()
    assert first['valid'] and canonical(first) == canonical(second)
    assert nested.raw_path.read_bytes() == before


def test_actual_worker_start_shape_is_supported(nested):
    # Production process_started precedes observation of endpoint ticks/GID.
    # Worker stdout can be buffered: unrelated file order is not a global clock.
    f = nested
    for e in f.raw:
        if e['event'] == 'process_started':
            for key in ('endpoint_gid', 'endpoint_generation', 'process_start_ticks'):
                e.pop(key, None)
    f.sync()
    assert f.audit()['valid'], f.audit()['errors']


def actual_producer_relay(f, monkeypatch):
    """Real parent+child serialization; only kernel/process/runtime IO is doubled.

    No subprocess, namespace, DDS or ROS is started. In particular request,
    source files, envelope, receipt translation, manifest and descriptor are
    written by the actual producer, NOT reconstructed from interface samples.
    """
    import shutil
    from unittest.mock import Mock
    from r2f.runtime import topology_namespace as bridge, execution_boundary as boundary
    from r2f.runtime import discovery_network as network, ros2_pair

    shutil.rmtree(f.side)  # Remove only this test's fabricated nested sidecars.
    source = f.config['source_manifest']
    outer = f.ns_manifest['outer_controller']
    parent, child, control = Mock(), Mock(fileno=lambda: 10), Mock()
    process = Mock(pid=2, returncode=0)
    messages, child_messages = [], []
    real_open = bridge.os.open
    worker_by_pid = {w['pid']: w for w in f.workers}

    def identity(pid):
        worker = worker_by_pid[pid]
        return dict(deepcopy(f.proof['identity']), pid=pid,
                    start_ticks=worker['process_start_ticks'])

    def send(sock, packet, handles=None):
        (child_messages if sock is control else messages).append(deepcopy(packet))

    def receive(sock, timeout):
        if sock is control:
            if not any(m['kind'] == 'result' for m in child_messages):
                return {'kind': 'execute', 'config_sha256': digest(f.inner_config)}, (0, 0, 0), []
            return {'kind': 'result_ack'}, (0, 0, 0), []
        packet = child_messages.pop(0)
        return packet, (3, 1000, 1000), []

    def runtime(config, output):
        assert config == f.inner_config and output == f.child_root
        ctx = bridge.current_child_context()
        assert type(ctx) is bridge._ChildContext
        # Drive the real receipt producer with explicit kernel/lease doubles.
        def emit(event, **fields):
            candidates = [r for r in f.raw if r['event'] == event
                          and (event != 'topology_namespace_worker_bound'
                               or r['binding']['pid'] == fields['binding']['pid'])]
            assert len(candidates) == 1
            row = candidates[0]
            for key, value in fields.items():
                assert row[key] == value
            return deepcopy(row)
        ctx.leases_ready(object(), config, output, emit)
        for worker in f.workers:
            handle = {k: worker[k] for k in bridge._WORKER_KEYS}
            handle['process'] = Mock(process=Mock(pid=worker['pid'], poll=lambda: None))
            ctx.worker_created(handle, emit)
        return deepcopy(f.child)

    def popen(argv, **kwargs):
        assert kwargs['start_new_session'] is True
        assert argv[-2] == bridge._sha(f.side / 'request.json')
        status = bridge._child(argv[-3], argv[-2], int(argv[-1]))
        assert status == 0, (f.side / 'child-failure.json').read_text() if status else ''
        return process

    with monkeypatch.context() as m:
        m.setattr(bridge, '_outer_gate', lambda *a: deepcopy(outer))
        m.setattr(bridge, '_bind_controller', lambda *a: deepcopy(f.nested_binding))
        m.setattr(bridge, '_cleanup', lambda *a: deepcopy(f.ns_manifest['cleanup']))
        m.setattr(boundary, '_same_source', lambda value: deepcopy(source))
        m.setattr(boundary, '_observe_owned_tree', Mock())
        m.setattr(boundary, '_pair', lambda: (parent, child))
        m.setattr(boundary, '_dumpable', Mock())
        m.setattr(boundary, '_nested_proc_scope', lambda value: deepcopy(f.proof))
        m.setattr(boundary, '_identity', identity)
        m.setattr(boundary, '_ticks', lambda pid: worker_by_pid[pid]['process_start_ticks'])
        m.setattr(network, '_host_identity', lambda: {
            'host_uid': 1000, 'host_cap_eff': 0, 'launcher_pid': 1,
            'host_net_namespace': outer['namespaces']['net'],
            'host_user_namespace': outer['namespaces']['user']})
        m.delenv(network._PROOF_ENV, raising=False)
        m.setattr(bridge.socket, 'socket', lambda **kw: control)
        m.setattr(bridge.os, 'pidfd_open', lambda pid: real_open('/dev/null', bridge.os.O_RDONLY))
        m.setattr(bridge.os, 'open', lambda *a: real_open('/dev/null', bridge.os.O_RDONLY))
        m.setattr(boundary, '_send', send)
        m.setattr(boundary, '_recv', receive)
        m.setattr(bridge.subprocess, 'Popen', popen)
        m.setattr(ros2_pair, 'verify_pair_lease', lambda *a: deepcopy(f.inner_leases))
        m.setattr(ros2_pair, 'run_pair', runtime)
        relay = bridge.run_isolated_topology(f.inner_config, f.child_root, boundary_request=f.request)
    assert [m['kind'] for m in messages] == ['execute', 'result_ack'], json.loads((f.side / 'manifest.json').read_bytes()).get('error')
    assert 'topology_namespace' in relay and relay.get('status') != 'blocked', relay
    f.inner = relay
    f.seal_outer()
    return json.loads((f.side / 'manifest.json').read_bytes())


def test_actual_producer_parent_child_artifacts_pass_consumer(nested, monkeypatch):
    manifest = actual_producer_relay(nested, monkeypatch)
    request = json.loads((nested.side / 'request.json').read_bytes())
    assert request.get('bridge_id') == manifest['bridge_id']
    envelope = json.loads((nested.side / 'child-envelope.json').read_bytes())
    assert type(envelope['inner_lease_scope']['raw_event_ref']) is int
    assert isinstance(manifest['inner_lease_scope']['raw_event_ref'], dict)
    raw_before = nested.raw_path.read_bytes()
    audit = nested.audit()
    assert audit['valid'], audit['errors']
    assert audit['inner_artifact_view']['result_ref'] == manifest['inner_result_ref']
    assert nested.raw_path.read_bytes() == raw_before
    assert not audit['real_execution_attested'] and not audit['candidate_eligible']


@pytest.mark.parametrize('damage', ['request_bridge_missing', 'request_bridge_wrong', 'envelope_integer_ref'])
def test_actual_producer_artifacts_rehashed_tamper_fails_consumer(nested, monkeypatch, damage):
    manifest = actual_producer_relay(nested, monkeypatch)
    if damage.startswith('request_bridge'):
        path = nested.side / 'request.json'
        value = json.loads(path.read_bytes())
        if damage == 'request_bridge_missing': value.pop('bridge_id', None)
        else: value['bridge_id'] = 'f' * 32
        write(path, value)
        manifest['request_ref'] = file_ref(path)
        manifest['command'][-2] = manifest['request_ref']['sha256']
    else:
        path = nested.side / 'child-envelope.json'
        value = json.loads(path.read_bytes())
        value['worker_bindings'][0]['raw_event_ref'] = manifest['worker_bindings'][0]['raw_event_ref']
        write(path, value)
        manifest['envelope_ref'] = file_ref(path)
    write(nested.side / 'manifest.json', manifest)
    nested.inner['topology_namespace']['manifest_sha256'] = file_ref(nested.side / 'manifest.json')['sha256']
    nested.seal_outer()
    audit = nested.audit()
    assert not audit['valid']
    assert 'inner_artifact_view' not in audit


def test_actual_producer_core_v2_source_mismatch_remains_fail_closed(nested, monkeypatch):
    # Preserve the exact integration-first regression as an explicit negative:
    # parent/child IO doubles must never bypass the real core route validator.
    nested.inner_config['source_manifest'] = dict(nested.inner_config['source_manifest'], head='c' * 40)
    with pytest.raises(AssertionError, match='topology_v2_binding_mismatch:source_manifest'):
        actual_producer_relay(nested, monkeypatch)
    failure = json.loads((nested.side / 'child-failure.json').read_bytes())
    assert failure == {'error': 'ValueError:topology_v2_binding_mismatch:source_manifest'}
    manifest = json.loads((nested.side / 'manifest.json').read_bytes())
    assert manifest['error'] is not None
    assert not (nested.side / 'child-runtime-result.json').exists()
