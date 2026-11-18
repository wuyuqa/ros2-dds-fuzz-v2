"""Read-only execution-boundary artifact consistency, never live attestation.

This helper does not import the namespace executor, acquire leases, inspect
/proc, invoke a runtime, or establish kernel/ROS/worker eligibility. ``valid``
only means the supplied artifacts agree under the current v1 writer contract.
Callers still need their exact canonical callable and independent runtime,
raw/environment/worker qualification. Synthetic fixtures can pass consistency
without having executed anything. Guardian JSONL and supervisor-final are not
sealed by the guardian manifest; their declarations are not authentication.
"""
from __future__ import annotations

import copy
import hashlib
import json
from pathlib import Path
import re

SCHEMA = 'r2f.execution-boundary-audit/v1'
BOUNDARY_SCHEMA = 'r2f.execution-boundary/v1'
AUTHENTICATION = 'SCM_CREDENTIALS+SCM_RIGHTS+pidfd+proc_start_ticks+NS_GET_NSTYPE'
CLEARANCE_SCOPE = 'authenticated_independent_user_mount_pid_net_boundary_only'
CLEANUP_METHOD = 'pidfd-bound PID1 exit + launcher wait; kernel PIDNS teardown, not host PID scan'
PID_INTERPRETATION = 'raw pid/pgid are namespace identifiers, NOT host identifiers'
NAMESPACES = ('user', 'mnt', 'pid', 'net')


def _encoded(value, *, ensure_ascii=True):
    return json.dumps(value, sort_keys=True, separators=(',', ':'),
                      allow_nan=False, ensure_ascii=ensure_ascii).encode()


def _hash(value, **kwargs):
    return hashlib.sha256(_encoded(value, **kwargs)).hexdigest()


def _equal(left, right):
    # Unlike Python equality this does not equate True and 1.
    return _encoded(left) == _encoded(right)


def _pairs(pairs):
    value = {}
    for key, item in pairs:
        if key in value:
            raise ValueError('duplicate JSON key: ' + key)
        value[key] = item
    return value


def _decode(data):
    def bad_constant(value):
        raise ValueError('nonfinite JSON constant: ' + value)
    return json.loads(data, object_pairs_hook=_pairs, parse_constant=bad_constant)


class _Audit:
    def __init__(self):
        self.errors, self.references, self.contents = [], {}, {}
        self.root = None
        self.inner_artifact_view = None

    def check(self, condition, code, field, detail):
        if not condition:
            self.errors.append({'code': code, 'field': field, 'detail': detail})
        return bool(condition)

    def equal(self, left, right, field):
        return self.check(_equal(left, right), 'contract_gap' if left is None and right is not None else 'identity_mismatch',
                          field, 'required value missing/null' if left is None and right is not None
                          else 'values differ (type-sensitive JSON)')

    def obj(self, value, field):
        return value if self.check(isinstance(value, dict), 'contract_gap', field, 'object required') else {}

    def seq(self, value, field):
        return value if self.check(isinstance(value, list), 'contract_gap', field, 'array required') else []

    def positive(self, value, field):
        return self.check(type(value) is int and value > 0, 'invalid_integer', field, 'positive integer required, not bool')

    def digest(self, value, field):
        return self.check(isinstance(value, str) and re.fullmatch('[0-9a-f]{64}', value) is not None,
                          'contract_gap', field, 'lowercase SHA256 required')

    def path(self, value, field, *, within=None, exists=True):
        if not self.check(isinstance(value, str) and bool(value) and Path(value).is_absolute(),
                          'invalid_path', field, 'absolute filesystem path required'):
            return None
        try:
            path = Path(value).resolve(strict=exists)
            if within is not None and not self.check(path.is_relative_to(within), 'path_escape', field,
                                                     'resolved path is outside ' + str(within)):
                return None
            return path
        except (OSError, ValueError, RuntimeError) as exc:
            self.check(False, 'unreadable_reference', field, type(exc).__name__ + ':' + str(exc))
            return None

    def read(self, value, field, expected=None, *, within=None, seal_required=False, kind='json'):
        path = self.path(value, field, within=within or self.root)
        if path is None:
            return {} if kind == 'json' else [] if kind == 'jsonl' else None
        try:
            data = path.read_bytes()
            sha = hashlib.sha256(data).hexdigest()
            previous = self.contents.setdefault(path, data)
            self.check(previous == data, 'artifact_changed', field, 'reference changed during audit')
            self.references[field] = {'path': str(path), 'sha256': sha,
                                      'expected_sha256': expected}
            if seal_required or expected is not None:
                if self.digest(expected, field + '.sha256'):
                    self.equal(sha, expected, field + '.sha256')
            if kind == 'bytes':
                return data
            if kind == 'jsonl':
                rows = [_decode(line) for line in data.splitlines() if line.strip()]
                self.check(all(isinstance(row, dict) for row in rows), 'invalid_json', field, 'JSONL rows must be objects')
                return [row for row in rows if isinstance(row, dict)]
            return self.obj(_decode(data), field)
        except (OSError, ValueError, TypeError, RecursionError) as exc:
            self.check(False, 'invalid_artifact', field, type(exc).__name__ + ':' + str(exc))
            return {} if kind == 'json' else [] if kind == 'jsonl' else None

    def identity(self, value, field):
        value = self.obj(value, field)
        self.positive(value.get('pid'), field + '.pid')
        self.positive(value.get('start_ticks'), field + '.start_ticks')
        self.equal(value.get('uid'), 1000, field + '.uid')
        self.equal(value.get('cap_eff'), 0, field + '.cap_eff')
        ns = self.obj(value.get('namespaces'), field + '.namespaces')
        for name in NAMESPACES:
            self.check(isinstance(ns.get(name), str) and re.fullmatch(name + r':\[[0-9]+\]', ns[name]) is not None,
                       'contract_gap', field + '.namespaces.' + name, 'typed namespace identity required')
        pids = self.seq(value.get('nspid'), field + '.nspid')
        self.check(bool(pids) and all(type(pid) is int and pid > 0 for pid in pids),
                   'contract_gap', field + '.nspid', 'namespace PID chain required')
        if pids:
            self.equal(pids[0], value.get('pid'), field + '.nspid.host')
        for name in ('uid_map', 'gid_map'):
            self.mapping(value.get(name), field + '.' + name)
        return value

    def mapping(self, value, field):
        try:
            if not isinstance(value, str):
                raise ValueError('mapping must be text')
            rows = [[int(item) for item in line.split()] for line in value.splitlines() if line.strip()]
            if not rows or any(len(row) != 3 or min(row[:2]) < 0 or row[2] <= 0 or
                               max(row[0] + row[2], row[1] + row[2]) > 2**32 for row in rows):
                raise ValueError('invalid mapping rows')
            return rows
        except (TypeError, ValueError) as exc:
            self.check(False, 'contract_gap', field, str(exc))
            return []

    def binding(self, value, host, field, *, controller=True, controller_ns=None):
        value = self.obj(value, field)
        self.equal(value.get('authentication'), AUTHENTICATION, field + '.authentication')
        inner = self.identity(value.get('host'), field + '.host')
        ns = self.obj(inner.get('namespaces'), field + '.host.namespaces')
        host_ns = self.obj(host.get('namespaces'), 'host.namespaces')
        for name in NAMESPACES:
            self.check(ns.get(name) is not None and ns.get(name) != host_ns.get(name),
                       'namespace_not_separated', field + '.' + name, 'host and inner namespaces must differ')
            if controller_ns is not None:
                self.equal(ns.get(name), controller_ns.get(name), field + '.controller_namespace.' + name)
        pids = inner.get('nspid', [])
        if isinstance(pids, list) and pids:
            self.equal(value.get('inner_pid'), pids[-1], field + '.inner_pid.nspid')
        self.check(isinstance(pids, list) and len(pids) >= 2, 'contract_gap', field + '.host.nspid',
                   'host-visible to inner namespace PID chain required')
        if controller:
            self.equal(value.get('inner_pid'), 1, field + '.inner_pid')
        else:
            self.check(type(value.get('inner_pid')) is int and value['inner_pid'] > 1,
                       'invalid_descendant', field, 'descendant PID must be greater than one')
        self.equal(value.get('pid_scope'), ns.get('pid'), field + '.pid_scope')
        self.equal(self.mapping(inner.get('uid_map'), field + '.uid_map'), [[1000, 1000, 1]], field + '.uid_map')
        gids = self.mapping(inner.get('gid_map'), field + '.gid_map')
        self.check(len(gids) == 1 and gids[0][0] == gids[0][1] and gids[0][2] == 1,
                   'invalid_gid_map', field + '.gid_map', 'single identity GID mapping required; peer GID is not serialized')
        loopback = self.obj(value.get('loopback_report'), field + '.loopback_report')
        self.check(loopback.get('up') is True, 'contract_gap', field + '.loopback_report.up', 'loopback readiness declaration required')
        return value, inner

    def leases(self, value, field, root, state, config, pid, *, host=None, boundary_id=None):
        rows = self.seq(value, field)
        self.check(len(rows) == 3, 'lease_count', field, 'exactly three lease records required')
        if not isinstance(config.get('campaign_id'), str):
            self.check(False, 'contract_gap', 'config.campaign_id', 'campaign identity required')
            return
        names = ['campaign-' + hashlib.sha256(config['campaign_id'].encode()).hexdigest() + '.lock',
                 'domain-' + str(config.get('domain_id')) + '.lock',
                 'state-' + hashlib.sha256(str(state).encode()).hexdigest() + '.lock']
        for index, row in enumerate(rows):
            label = field + '[' + str(index) + ']'
            row = self.obj(row, label)
            self.check(row.get('acquired') is True, 'lease_not_acquired', label, 'acquired must be true')
            owner = self.obj(row.get('owner'), label + '.owner')
            expected = {'pid': pid, 'campaign_id': config.get('campaign_id'),
                        'domain_id': config.get('domain_id'), 'state_dir': str(state)}
            for key, wanted in expected.items():
                self.equal(owner.get(key), wanted, label + '.owner.' + key)
            self.positive(owner.get('acquired_time_ns'), label + '.owner.acquired_time_ns')
            if root is not None and index < 3:
                self.equal(row.get('path'), str(root / names[index]), label + '.path')
            if host is not None:
                for key in ('inode', 'device'):
                    self.positive(row.get(key), label + '.' + key)
                for key, wanted in {'start_ticks': host.get('start_ticks'),
                                    'pid_namespace': host.get('namespaces', {}).get('pid'),
                                    'lease_scope': 'host_supervisor', 'boundary_id': boundary_id}.items():
                    self.equal(owner.get(key), wanted, label + '.owner.' + key)

    def finish(self):
        for path, original in self.contents.items():
            try:
                resolved = path.resolve(strict=True)
                if resolved != path or (self.root is not None and not resolved.is_relative_to(self.root)):
                    self.check(False, 'artifact_changed', str(path), 'reference resolution changed before audit finished')
                    continue
                self.check(path.read_bytes() == original, 'artifact_changed', str(path), 'content changed before audit finished')
            except (OSError, ValueError, RuntimeError) as exc:
                self.check(False, 'unreadable_reference', str(path), type(exc).__name__ + ':' + str(exc))
        result = {'schema': SCHEMA, 'valid': not self.errors, 'errors': self.errors,
                'contract_gaps': [row for row in self.errors if row['code'] == 'contract_gap'],
                'references': self.references, 'evidence_kind': 'artifact_consistency_only',
                'real_execution_attested': False, 'candidate_eligible': False,
                'limitations': ['No kernel/SCM/pidfd/namespace/lease reauthentication or live process scan.',
                               'Guardian JSONL and supervisor-final are auxiliary, not manifest-sealed attestation.',
                               'owned_process_observations is sampled, not a complete lifecycle scan.',
                               'Peer GID is not separately serialized; only declared single identity GID map is checked.',
                               'Source freeze/dependency identities are declarations, not current repository attestation.',
                               'Canonical callable, raw/environment/worker eligibility remain caller obligations.']}
        if not self.errors and self.inner_artifact_view is not None:
            result['inner_artifact_view'] = self.inner_artifact_view
        return result


def audit_execution_boundary(config, result, *, output_root=None):
    """Audit immutable file references/declarations only; never execute a backend.

    ``output_root`` pins artifact containment; omitted roots are inferred from
    the manifest's standard ``output/execution-boundary-ID/manifest.json`` path
    and are not an independent trust anchor. Missing/unprovable required v1
    fields fail closed, with explicit ``contract_gap`` diagnostics.
    """
    audit = _Audit()
    try:
        _audit(audit, config, result, output_root)
    except (OSError, ValueError, TypeError, KeyError, IndexError, AttributeError, RecursionError) as exc:
        audit.check(False, 'malformed_artifact', 'audit', type(exc).__name__ + ':' + str(exc))
    return audit.finish()


def _audit(a, config, result, output_root):
    config, result = a.obj(config, 'config'), a.obj(result, 'result')
    # Reject non-JSON inputs rather than mutating/coercing them.
    _encoded(config)
    _encoded(result)
    context = a.obj(result.get('execution_boundary'), 'result.execution_boundary')
    manifest_path = a.path(context.get('manifest_path'), 'context.manifest_path')
    if manifest_path is None:
        return
    a.root = a.path(str(output_root) if output_root is not None else str(manifest_path.parent.parent), 'output_root')
    if a.root is None:
        return
    manifest = a.read(str(manifest_path), 'manifest', context.get('manifest_sha256'), seal_required=True)
    directory = manifest_path.parent
    boundary_id = context.get('boundary_id')
    a.check(isinstance(boundary_id, str) and re.fullmatch('[0-9a-f]{32}', boundary_id) is not None,
            'contract_gap', 'context.boundary_id', '32 lowercase hex boundary identity required')
    a.equal(directory.name, 'execution-boundary-' + str(boundary_id), 'manifest.directory')
    a.equal(manifest_path.name, 'manifest.json', 'manifest.filename')
    hashes = a.obj(manifest.get('artifact_hashes'), 'manifest.artifact_hashes')
    a.check('guardian-events.jsonl' not in hashes, 'guardian_hash_scope_mismatch', 'manifest.artifact_hashes',
            'current v1 writer excludes guardian JSONL; do not upgrade auxiliary chronology')
    for name, sha in hashes.items():
        if a.check(isinstance(name, str) and name not in ('.', '..') and Path(name).name == name and '\\' not in name,
                   'path_escape', 'manifest.artifact_hashes', 'artifact names must be single file components'):
            a.read(str(directory / name), 'artifact:' + name, sha, within=directory, seal_required=True, kind='bytes')

    def sealed(name):
        a.check(name in hashes, 'contract_gap', 'manifest.artifact_hashes.' + name, 'required artifact is not manifest-sealed')
        return a.read(str(directory / name), name, hashes.get(name), within=directory, seal_required=True)

    request, submitted = sealed('request.json'), sealed('submitted-config.json')
    inner, inner_leases = sealed('inner-runtime-result.json'), sealed('inner-leases.json')
    launcher_cleanup = sealed('launcher-cleanup.json')
    final = a.read(str(directory / 'supervisor-final.json'), 'supervisor-final.json', within=directory)
    raw_identity_path = a.path(context.get('raw_identity_path'), 'context.raw_identity_path', within=a.root)
    if raw_identity_path is not None:
        a.equal(str(raw_identity_path), str(directory / 'raw-identity.json'), 'context.raw_identity_path')
    raw_id = a.read(context.get('raw_identity_path'), 'raw-identity', context.get('raw_identity_sha256'), seal_required=True)
    persisted = a.read(str(a.root / 'runtime_result.json'), 'wrapper-runtime-result.json')
    a.equal(persisted, result, 'wrapper-runtime-result.argument')
    for label, value in [('context', context), ('manifest', manifest), ('request', request), ('raw-identity', raw_id)]:
        a.equal(value.get('schema'), BOUNDARY_SCHEMA, label + '.schema')
        a.equal(value.get('boundary_id'), boundary_id, label + '.boundary_id')
    for label, value in [('context', context), ('manifest', manifest), ('request', request)]:
        a.check(value.get('host_clearance_claimed') is False, 'host_clearance_forbidden', label, 'must explicitly remain false')
        a.equal(value.get('clearance_scope'), CLEARANCE_SCOPE, label + '.clearance_scope')
    for label, value in [('context', context), ('manifest', manifest)]:
        a.check(value.get('nonDDS') is False, 'nonDDS_not_eligible', label + '.nonDDS', 'only explicit false is eligible')
    a.check('error' in manifest and manifest['error'] is None, 'boundary_error', 'manifest.error', 'explicit null error required')
    a.check('probe' in request and request['probe'] is None, 'nonDDS_not_eligible', 'request.probe', 'formal request has probe=null')
    a.check(a.obj(config.get('execution_boundary'), 'config.execution_boundary').get('enabled') is True,
            'boundary_not_enabled', 'config.execution_boundary.enabled', 'must be true')
    a.equal(context.get('maturity_credit'), 'decided_by_unified_runner', 'context.maturity_credit')
    a.check(manifest.get('no_new_scope_or_maturity_denominator') is True, 'contract_gap', 'manifest.no_new_scope_or_maturity_denominator', 'scope declaration required')
    a.check(manifest.get('source_freeze_credit') is True, 'unfrozen_source', 'manifest.source_freeze_credit', 'formal source freeze declaration required')
    source = a.obj(config.get('source_manifest'), 'config.source_manifest')
    a.check(source.get('source_frozen') is True and source.get('dirty_behavior_paths') == [],
            'unfrozen_source', 'config.source_manifest', 'frozen source with explicit empty dirty list required')
    a.check(isinstance(source.get('head'), str) and re.fullmatch('[0-9a-f]{40}', source['head']) is not None,
            'contract_gap', 'config.source_manifest.head', 'full source HEAD required')
    a.digest(source.get('behavior_tree_sha256'), 'config.source_manifest.behavior_tree_sha256')
    a.equal(source.get('head'), config.get('expected_head'), 'config.expected_head')
    a.equal(source.get('source_repo'), config.get('source_repo'), 'config.source_repo')
    a.equal(manifest.get('source_after'), source, 'manifest.source_after')
    identity = a.obj(manifest.get('identity'), 'manifest.identity')
    a.equal(request.get('identity'), identity, 'request.identity')
    a.equal(identity.get('source_manifest'), source, 'manifest.identity.source_manifest')
    a.equal(submitted, config, 'submitted-config')
    a.equal(identity.get('config_sha256'), _hash(config), 'manifest.identity.config_sha256')
    for key in ('dependency_lock_sha256', 'effective_input_sha256', 'target_sha256'):
        a.digest(identity.get(key), 'manifest.identity.' + key)
    a.equal(identity.get('effective_input_sha256'), config.get('effective_input_sha256'), 'manifest.identity.effective_input_sha256')
    scenario = a.obj(config.get('scenario'), 'config.scenario')
    target = a.obj(scenario.get('target'), 'config.scenario.target')
    a.equal(identity.get('target_sha256'), _hash(target, ensure_ascii=False), 'manifest.identity.target_sha256')
    # Use the actual pure normalizer: _supervise hashes normalized config before
    # changing lock_root; identity.config_sha256 hashes the original submission.
    from r2f.runtime.ros2_pair import normalize_config
    if not a.check(isinstance(config.get('run_id'), str), 'contract_gap', 'config.run_id',
                   'explicit run identity required for deterministic normalization'):
        return
    normalized = normalize_config(copy.deepcopy(config))
    a.equal(request.get('submitted_config_sha256'), _hash(normalized), 'request.submitted_config_sha256')
    host_root = a.path(normalized.get('lock_root', '/tmp/r2f-runtime-locks-1000'), 'config.lock_root', exists=False)
    inner_root = host_root / 'execution-boundaries' / str(boundary_id) / 'inner' if host_root else None
    expected_config = dict(normalized, lock_root=str(inner_root))
    a.equal(request.get('config'), expected_config, 'request.config.normalized_inner')
    a.equal(request.get('inner_config_sha256'), _hash(expected_config), 'request.inner_config_sha256')
    for label, value in [('context', context), ('manifest', manifest)]:
        a.equal(value.get('inner_lock_root'), str(inner_root), label + '.inner_lock_root')
    a.equal(request.get('output_dir'), str(a.root), 'request.output_dir')
    a.equal(request.get('audit_dir'), str(directory), 'request.audit_dir')
    a.equal(inner_leases.get('lock_root'), str(inner_root), 'inner-leases.lock_root')
    host = a.identity(manifest.get('host_supervisor'), 'manifest.host_supervisor')
    a.equal(request.get('host'), host, 'request.host')
    a.equal(context.get('host_supervisor'), host, 'context.host_supervisor')
    a.equal(final.get('host'), host, 'supervisor-final.host')
    guardian = a.identity(manifest.get('guardian'), 'manifest.guardian')
    a.equal(guardian.get('namespaces'), host.get('namespaces'), 'manifest.guardian.host_namespaces')
    a.equal(final.get('guardian_pid'), guardian.get('pid'), 'supervisor-final.guardian_pid')
    binding, controller = a.binding(manifest.get('controller_binding'), host, 'manifest.controller_binding')
    a.equal(context.get('controller_binding'), binding, 'context.controller_binding')
    a.equal(raw_id.get('controller_binding'), binding, 'raw-identity.controller_binding')
    pid_scope = binding.get('pid_scope')
    nested = any('topology_namespace' in value or 'nested_controller_binding' in value
                 for value in (result, inner, context, raw_id, inner_leases, manifest)) or 'outer_pid_scope' in inner_leases
    if not nested:
        a.equal(context.get('raw_pid_scope'), pid_scope, 'context.raw_pid_scope')
        a.equal(context.get('raw_network_scope'),
                'controller boundary; existing nested backend may create its own user/net namespace',
                'context.raw_network_scope')
        a.equal(raw_id.get('pid_namespace'), pid_scope, 'raw-identity.pid_namespace')
    a.equal(raw_id.get('pid_interpretation'), PID_INTERPRETATION, 'raw-identity.pid_interpretation')
    a.equal(raw_id.get('unobserved_worker_mapping'), 'unknown; never infer host PID from raw PID equality', 'raw-identity.unobserved_worker_mapping')
    observations = a.seq(manifest.get('owned_process_observations'), 'manifest.owned_process_observations')
    a.equal(raw_id.get('owned_process_observations'), observations, 'raw-identity.owned_process_observations')
    a.equal(manifest.get('owned_process_observation_limitation'), 'sampled supporting mapping, not complete lifecycle/host clearance',
            'manifest.owned_process_observation_limitation')
    if not nested:
        a.equal(inner_leases.get('pid_scope'), pid_scope, 'inner-leases.pid_scope')
    a.equal(context.get('host_leases'), manifest.get('host_leases'), 'context.host_leases')
    a.equal(request.get('host_leases'), manifest.get('host_leases'), 'request.host_leases')
    a.equal(context.get('inner_leases'), manifest.get('inner_leases'), 'context.inner_leases')
    a.equal(inner_leases.get('receipts'), manifest.get('inner_leases'), 'inner-leases.receipts')
    a.leases(manifest.get('host_leases'), 'host_leases', host_root, a.root, config, host.get('pid'), host=host, boundary_id=boundary_id)
    a.leases(manifest.get('inner_leases'), 'inner_leases', inner_root, a.root / 'inner-runtime', config, 1)
    cleanup = a.obj(manifest.get('cleanup'), 'manifest.cleanup')
    a.equal(context.get('cleanup'), cleanup, 'context.cleanup')
    for key in ('confirmed', 'pid1_exited', 'launcher_exited'):
        a.check(cleanup.get(key) is True, 'cleanup_unconfirmed', 'manifest.cleanup.' + key, 'explicit true required')
    a.equal(cleanup.get('launcher_returncode'), 0, 'manifest.cleanup.launcher_returncode')
    a.equal(cleanup.get('method'), CLEANUP_METHOD, 'manifest.cleanup.method')
    seen_descendants = set()
    for index, item in enumerate(a.seq(cleanup.get('descendants'), 'manifest.cleanup.descendants')):
        item = a.obj(item, 'cleanup.descendants[' + str(index) + ']')
        descendant, process = a.binding(item.get('binding'), host, 'cleanup.descendants[' + str(index) + '].binding',
                                       controller=False, controller_ns=controller.get('namespaces'))
        token = (process.get('pid'), process.get('start_ticks'))
        a.check(token not in seen_descendants, 'duplicate_descendant', 'manifest.cleanup.descendants', 'PID/start identity duplicated')
        seen_descendants.add(token)
        a.check(item.get('pidfd_exited') is True, 'cleanup_unconfirmed', 'manifest.cleanup.descendants[' + str(index) + ']', 'pidfd exit required')
    for key in ('pid1_reaped', 'init_pidfd_exited'):
        a.check(launcher_cleanup.get(key) is True, 'contract_gap', 'launcher-cleanup.' + key, 'sealed launcher waitpid/exit receipt required')
    a.positive(launcher_cleanup.get('launcher_pid'), 'launcher-cleanup.launcher_pid')
    ids = [host.get('pid'), guardian.get('pid'), controller.get('pid'), launcher_cleanup.get('launcher_pid')]
    a.check(len(set(ids)) == 4, 'process_identity_collision', 'host_processes', 'supervisor/guardian/controller/launcher must be distinct')
    a.check(final.get('leases_released') is True and final.get('leases_quarantined') is False,
            'leases_not_released', 'supervisor-final', 'leases_released=true and leases_quarantined=false required')
    times = [launcher_cleanup.get('monotonic_ns'), cleanup.get('monotonic_ns'), final.get('monotonic_ns')]
    a.check(all(type(t) is int and t > 0 for t in times) and times == sorted(times) if all(type(t) is int for t in times) else False,
            'cleanup_chronology', 'cleanup', 'launcher reap <= guardian cleanup <= supervisor release required')
    a.equal(manifest.get('runtime_result_sha256'), hashes.get('inner-runtime-result.json'), 'manifest.runtime_result_sha256')
    raw_path, raw_sha = inner.get('raw_path'), inner.get('raw_sha256')
    a.equal(raw_id.get('raw_path'), raw_path, 'raw-identity.raw_path')
    a.equal(raw_id.get('raw_sha256'), raw_sha, 'raw-identity.raw_sha256')
    raw = a.read(raw_path, 'runtime_raw', raw_sha, seal_required=True, kind='jsonl')
    a.equal(inner.get('events'), raw, 'inner.events.raw')
    lock_events = [row for row in raw if row.get('event') == 'isolation_locks_acquired']
    a.check(len(lock_events) == 1, 'raw_lease_event_missing', 'runtime_raw', 'one inner isolation_locks_acquired event required')
    if len(lock_events) == 1:
        a.equal(lock_events[0].get('locks'), manifest.get('inner_leases'), 'runtime_raw.inner_leases')
        a.equal(lock_events[0].get('pid'), 1, 'runtime_raw.controller_pid')
    for index, row in enumerate(raw):
        a.equal(row.get('event_index'), index, 'runtime_raw.event_index[' + str(index) + ']')
        a.equal(row.get('run_id'), config.get('run_id'), 'runtime_raw.run_id[' + str(index) + ']')
        a.equal(row.get('domain_id'), config.get('domain_id'), 'runtime_raw.domain_id[' + str(index) + ']')
    if nested:
        pid_scope = _audit_topology_namespace(a, config, expected_config, inner, context, raw_id,
                                             inner_leases, controller, pid_scope, raw, request.get('options'))
    expected_result = copy.deepcopy(inner)
    expected_result['execution_boundary'] = copy.deepcopy(context)
    for key in ('execution', 'execution_metadata'):
        value = expected_result.get(key)
        if isinstance(value, dict):
            value.update(pid_namespace=pid_scope, pid_interpretation='namespace_PID_not_host_PID')
            if isinstance(value.get('workers'), list):
                for worker in value['workers']:
                    a.obj(worker, 'inner.' + key + '.workers')
                    worker.update(pid_namespace=pid_scope, pid_interpretation='namespace_PID_not_host_PID')
    a.equal(result, expected_result, 'wrapper_result_projection')
    guardian_rows = a.read(str(directory / 'guardian-events.jsonl'), 'guardian_events_auxiliary', kind='jsonl', within=directory)
    required = ['host_lease_copies_authenticated', 'owned_launcher_started', 'pid1_kernel_bound_before_gate',
                'independent_boundary_authenticated', 'execute_authorized', 'runtime_result_received',
                'cleanup_begin', 'cleanup_observed', 'guardian_lease_copies_closing_after_cleanup']
    selected = {}
    indices = []
    for event in required:
        matches = [(i, row) for i, row in enumerate(guardian_rows) if row.get('event') == event]
        a.check(len(matches) == 1, 'contract_gap', 'guardian_events_auxiliary.' + event, 'one supporting chronology event required; not authentication')
        if len(matches) == 1:
            indices.append(matches[0][0])
            selected[event] = matches[0][1]
    a.check(indices == sorted(indices), 'guardian_chronology', 'guardian_events_auxiliary', 'phase order disagrees')
    stamps = [row.get('monotonic_ns') for row in guardian_rows]
    a.check(all(type(t) is int and t > 0 for t in stamps) and stamps == sorted(stamps) if all(type(t) is int for t in stamps) else False,
            'guardian_chronology', 'guardian_events_auxiliary', 'monotonic timestamps missing/reordered')
    for event, field, value in [('host_lease_copies_authenticated', 'receipts', manifest.get('host_leases')),
                                ('host_lease_copies_authenticated', 'owner', host),
                                ('host_lease_copies_authenticated', 'guardian', guardian),
                                ('pid1_kernel_bound_before_gate', 'start_ticks', controller.get('start_ticks')),
                                ('independent_boundary_authenticated', 'binding', binding),
                                ('execute_authorized', 'config_sha256', request.get('inner_config_sha256')),
                                ('runtime_result_received', 'sha256', manifest.get('runtime_result_sha256')),
                                ('cleanup_observed', 'receipt', cleanup),
                                ('owned_launcher_started', 'host_pid', launcher_cleanup.get('launcher_pid')),
                                ('pid1_kernel_bound_before_gate', 'host_pid', controller.get('pid'))]:
        if event in selected:
            a.equal(selected[event].get(field), value, 'guardian_events_auxiliary.' + event + '.' + field)
    if 'guardian_lease_copies_closing_after_cleanup' in selected:
        closed = selected['guardian_lease_copies_closing_after_cleanup']['monotonic_ns']
        if type(closed) is int and type(final.get('monotonic_ns')) is int:
            a.check(closed <= final['monotonic_ns'], 'lease_release_chronology', 'supervisor-final.monotonic_ns', 'host leases released before guardian close declaration')


TOPOLOGY_NAMESPACE_SCHEMA = 'r2f.topology-namespace/v1'
TOPOLOGY_MANIFEST_SCHEMA = 'r2f.topology-namespace-manifest/v1'
NESTED_CLEANUP_METHOD = 'pidfd-bound_nested_PID1_exit+unshare_wait+kernel_PIDNS_teardown'


def _audit_topology_namespace(a, config, inner_config, relay, context, raw_id,
                              inner_leases, controller, outer_scope, raw, boundary_options):
    """Check the optional producer sidecar DAG, not root flags or kernel state.

    All references are pinned below the caller's *fixed* inner-runtime root.
    A successful view is returned only by finish(), after the legacy outer
    controller/lease/cleanup and wrapper projection have also been checked.
    """
    label = 'topology_namespace'
    from r2f.modules.s1.topology_targets import TOPOLOGY_TARGET
    a.equal(config.get('target_id'), TOPOLOGY_TARGET, label + '.target_id')
    root = a.root / 'inner-runtime'
    sidecar = root / 'topology-namespace'
    desc = a.obj(relay.get(label), label)
    a.equal(desc.get('schema'), TOPOLOGY_NAMESPACE_SCHEMA, label + '.schema')
    descriptor_keys = {'schema', 'bridge_id', 'manifest_path', 'manifest_sha256', 'outer_controller',
                       'nested_controller_binding', 'raw_pid_scope', 'inner_lease_scope', 'worker_bindings', 'cleanup'}
    a.check(descriptor_keys <= set(desc) <= descriptor_keys | {'config_sha256', 'child_result_sha256'},
            'contract_gap', label + '.keys', 'unknown or missing descriptor fields')
    a.equal(desc.get('manifest_path'), str(sidecar / 'manifest.json'), label + '.manifest_path')
    manifest = a.read(desc.get('manifest_path'), label + '.manifest', desc.get('manifest_sha256'),
                      within=sidecar, seal_required=True)
    a.equal(manifest.get('schema'), TOPOLOGY_MANIFEST_SCHEMA, label + '.manifest.schema')
    a.equal(sorted(manifest), sorted({'schema', 'bridge_id', 'output_root', 'host_clearance_claimed',
        'maturity_credit', 'error', 'nested_controller_binding', 'raw_pid_scope', 'inner_lease_scope',
        'worker_bindings', 'parent_process_observations', 'outer_controller', 'source_manifest',
        'config_sha256', 'request_ref', 'command', 'envelope_ref', 'inner_result_ref',
        'source_before_ref', 'source_after_ref', 'raw_ref', 'cleanup'}), label + '.manifest.keys')
    for forbidden in ('manifest_path', 'manifest_sha256', 'topology_namespace'):
        a.check(forbidden not in manifest, 'circular_reference', label + '.manifest.' + forbidden,
                'manifest must not contain its own descriptor')
    a.check('error' in manifest and manifest['error'] is None, 'nested_error', label + '.error', 'explicit null required')
    a.equal(manifest.get('host_clearance_claimed'), False, label + '.host_clearance_claimed')
    a.equal(manifest.get('maturity_credit'), False, label + '.maturity_credit')
    a.equal(manifest.get('output_root'), str(root), label + '.output_root')
    bridge = manifest.get('bridge_id')
    a.check(isinstance(bridge, str) and re.fullmatch('[0-9a-f]{32}', bridge) is not None,
            'contract_gap', label + '.bridge_id', '32 lowercase hex bridge identity required')
    # Descriptor duplicates are a deliberately small projection, not a second
    # unsealed authority. In particular a missing manifest cannot be replaced
    # by root/capability flags in the result.
    for key in ('bridge_id', 'cleanup', 'inner_lease_scope',
                'nested_controller_binding', 'outer_controller', 'raw_pid_scope', 'worker_bindings'):
        a.equal(desc.get(key), manifest.get(key), label + '.descriptor.' + key)
    for field, value in [('context', context), ('raw-identity', raw_id)]:
        a.equal(value.get(label), desc, field + '.' + label)
    a.equal(manifest.get('config_sha256'), _hash(inner_config), label + '.config_sha256')
    source = config.get('source_manifest')
    a.equal(manifest.get('source_manifest'), source, label + '.source_manifest')

    def ref(key, filename, kind='json'):
        value = a.obj(manifest.get(key), label + '.' + key)
        a.equal(sorted(value), ['kind', 'path', 'sha256'], label + '.' + key + '.keys')
        a.equal(value.get('kind'), kind, label + '.' + key + '.kind')
        expected_path = root / filename
        a.equal(value.get('path'), str(expected_path), label + '.' + key + '.path')
        return a.read(value.get('path'), label + '.' + key, value.get('sha256'),
                      within=root, seal_required=True, kind=kind)

    request = ref('request_ref', 'topology-namespace/request.json')
    a.equal(request.get('options'), boundary_options, label + '.request.options')
    _audit_nested_command(a, manifest, request)
    before = ref('source_before_ref', 'topology-namespace/source-before.json')
    after = ref('source_after_ref', 'topology-namespace/source-after.json')
    child = ref('inner_result_ref', 'topology-namespace/child-runtime-result.json')
    envelope = ref('envelope_ref', 'topology-namespace/child-envelope.json')
    referenced_raw = ref('raw_ref', 'raw.jsonl', 'jsonl')
    a.equal(before, source, label + '.source_before')
    a.equal(after, source, label + '.source_after')
    a.equal(referenced_raw, raw, label + '.raw')
    raw_ref = a.obj(manifest.get('raw_ref'), label + '.raw_ref')
    a.equal(child.get('raw_path'), raw_ref.get('path'), label + '.child.raw_path')
    a.equal(child.get('raw_sha256'), raw_ref.get('sha256'), label + '.child.raw_sha256')
    a.equal(child.get('events'), raw, label + '.child.events')
    if 'config_sha256' in desc:
        a.equal(desc['config_sha256'], manifest.get('config_sha256'), label + '.descriptor.config_sha256')
    if 'child_result_sha256' in desc:
        a.equal(desc['child_result_sha256'], a.obj(manifest.get('inner_result_ref'), label + '.inner_result_ref').get('sha256'),
                label + '.child_result_sha256')
    expected_relay = copy.deepcopy(child)
    expected_relay[label] = copy.deepcopy(desc)
    a.equal(relay, expected_relay, label + '.relay_projection')
    a.check('execution_boundary' not in child and label not in child, 'recursive_wrapper', label + '.child',
            'child result must be original, unannotated canonical result')

    outer = a.obj(manifest.get('outer_controller'), label + '.outer_controller')
    # The enclosing binding is host-visible, whereas outer_controller was
    # sampled from outer PIDNS PID1. Compare scope plus start ticks, not PID1.
    a.equal(outer.get('pid'), 1, label + '.outer_controller.pid')
    a.equal(outer.get('nspid'), [1], label + '.outer_controller.nspid')
    for key in ('uid', 'cap_eff', 'start_ticks', 'namespaces', 'uid_map', 'gid_map'):
        a.equal(outer.get(key), controller.get(key), label + '.outer_controller.' + key)
    binding = a.obj(manifest.get('nested_controller_binding'), label + '.nested_controller_binding')
    a.equal(sorted(binding), sorted({'authentication', 'parent_pid_scope', 'outer', 'reported_inner',
                                    'inner_pid', 'pid_scope', 'pid_namespace_chain'}), label + '.binding.keys')
    a.equal(binding.get('authentication'), AUTHENTICATION, label + '.authentication')
    a.equal(binding.get('parent_pid_scope'), outer_scope, label + '.parent_pid_scope')
    ancestor = a.obj(binding.get('outer'), label + '.binding.outer')
    reported = a.obj(binding.get('reported_inner'), label + '.binding.reported_inner')
    scope = binding.get('pid_scope')
    namespaces = a.obj(reported.get('namespaces'), label + '.binding.namespaces')
    for name in NAMESPACES:
        value = namespaces.get(name)
        a.check(isinstance(value, str) and re.fullmatch(name + r':\[[0-9]+\]', value) is not None,
                'contract_gap', label + '.namespaces.' + name, 'typed nested namespace required')
        a.check(value is not None and value != outer.get('namespaces', {}).get(name),
                'namespace_not_separated', label + '.namespaces.' + name, 'nested and outer must differ')
    a.equal(ancestor.get('namespaces'), namespaces, label + '.binding.outer.namespaces')
    a.equal(scope, namespaces.get('pid'), label + '.binding.pid_scope')
    a.equal(binding.get('inner_pid'), 1, label + '.binding.inner_pid')
    a.check(type(ancestor.get('pid')) is int and ancestor['pid'] > 1, 'invalid_descendant',
            label + '.binding.outer.pid', 'nested PID1 must be a distinct outer descendant')
    a.equal(ancestor.get('nspid'), [ancestor.get('pid'), 1], label + '.binding.outer.nspid')
    a.equal(reported.get('pid'), 1, label + '.binding.reported_inner.pid')
    a.equal(reported.get('nspid'), [1], label + '.binding.reported_inner.nspid')
    a.equal(ancestor.get('uid'), 1000, label + '.binding.outer.uid')
    a.equal(reported.get('uid'), 0, label + '.binding.reported_inner.uid')
    a.positive(reported.get('start_ticks'), label + '.binding.start_ticks')
    a.equal(ancestor.get('start_ticks'), reported.get('start_ticks'), label + '.binding.outer.start_ticks')
    a.positive(reported.get('cap_eff'), label + '.binding.cap_eff')
    a.equal(ancestor.get('cap_eff'), reported.get('cap_eff'), label + '.binding.outer.cap_eff')
    a.check(type(reported.get('cap_eff')) is int and bool(reported['cap_eff'] & (1 << 12)),
            'contract_gap', label + '.binding.cap_net_admin', 'nested CAP_NET_ADMIN declaration required')
    outer_gids = a.mapping(outer.get('gid_map'), label + '.outer_controller.gid_map')
    expected_gid = outer_gids[0][0] if len(outer_gids) == 1 else None
    for who, identity in [('outer', ancestor), ('reported_inner', reported)]:
        for key, wanted in [('uid_map', [[0, 1000, 1]]), ('gid_map', [[0, expected_gid, 1]])]:
            a.equal(a.mapping(identity.get(key), label + '.binding.' + who + '.' + key),
                    wanted, label + '.binding.' + who + '.' + key)
    a.equal(binding.get('pid_namespace_chain'), [{'pid': ancestor.get('pid'), 'pid_scope': outer_scope},
              {'pid': 1, 'pid_scope': scope}], label + '.binding.pid_namespace_chain')
    a.equal(manifest.get('raw_pid_scope'), scope, label + '.raw_pid_scope')
    for field, value in [('context', context), ('raw-identity', raw_id), ('inner-leases', inner_leases)]:
        a.equal(value.get('nested_controller_binding'), binding, field + '.nested_controller_binding')
    a.equal(context.get('raw_pid_scope'), scope, 'context.raw_pid_scope')
    a.equal(context.get('raw_network_scope'), 'authenticated nested topology controller and workers',
            'context.raw_network_scope')
    a.equal(raw_id.get('pid_namespace'), scope, 'raw-identity.pid_namespace')
    a.equal(inner_leases.get('pid_scope'), scope, 'inner-leases.pid_scope')
    a.equal(inner_leases.get('outer_pid_scope'), outer_scope, 'inner-leases.outer_pid_scope')
    cleanup = a.obj(manifest.get('cleanup'), label + '.cleanup')
    for key in ('confirmed', 'controller_exited'):
        a.equal(cleanup.get(key), True, label + '.cleanup.' + key)
    a.equal(cleanup.get('method'), NESTED_CLEANUP_METHOD, label + '.cleanup.method')
    a.equal(cleanup.get('wrapper_returncode'), 0, label + '.cleanup.wrapper_returncode')

    def event_ref(value, field, event):
        value = a.obj(value, field)
        a.equal(sorted(value), ['event_index', 'path', 'sha256'], field + '.keys')
        for key in ('path', 'sha256'):
            a.equal(value.get(key), raw_ref.get(key), field + '.' + key)
        index = value.get('event_index')
        if not a.check(type(index) is int and 0 <= index < len(raw), 'contract_gap', field + '.event_index', 'raw index required'):
            return {}
        row = raw[index]
        a.equal(row.get('event'), event, field + '.event')
        return row

    leases = a.obj(manifest.get('inner_lease_scope'), label + '.inner_lease_scope')
    for key, expected in [('owner_pid', 1), ('pid_scope', scope), ('state_dir', str(root)),
                          ('lock_root', inner_config.get('lock_root')),
                          ('controller_start_ticks', reported.get('start_ticks')),
                          ('receipts', inner_leases.get('receipts'))]:
        a.equal(leases.get(key), expected, label + '.inner_lease_scope.' + key)
    ready = event_ref(leases.get('raw_event_ref'), label + '.lease_raw_ref', 'topology_namespace_ready')
    a.equal(ready.get('lease_scope'), {k: v for k, v in leases.items() if k != 'raw_event_ref'},
            label + '.lease_raw_ref.lease_scope')
    # Precise request/envelope and readiness/worker receipt fields are kept in
    # one routine, so absent producer fields cannot silently fall back to flags.
    _audit_nested_receipts(a, manifest, request, envelope, child, raw, binding, event_ref, inner_config, root, ready)
    metadata = a.obj(child.get('execution_metadata'), label + '.child.execution_metadata')
    disk_metadata = a.read(str(root / 'execution_metadata.json'), label + '.metadata', within=root)
    a.equal(disk_metadata, metadata, label + '.metadata.child')
    recorded = [e for e in raw if e.get('event') == 'topology_execution_metadata']
    a.check(len(recorded) == 1, 'contract_gap', label + '.metadata.raw', 'one raw metadata receipt required')
    if len(recorded) == 1:
        a.equal(recorded[0].get('metadata'), metadata, label + '.metadata.raw')
    env_ref = a.obj(metadata.get('environment_manifest'), label + '.environment_ref')
    a.equal(env_ref.get('path'), str(root / 'environment.json'), label + '.environment_ref.path')
    environment = a.read(env_ref.get('path'), label + '.environment', env_ref.get('sha256'),
                         within=root, seal_required=True)
    a.equal(environment, child.get('environment'), label + '.environment.child')
    a.inner_artifact_view = {'schema': 'r2f.audited-topology-inner-view/v1', 'output_root': str(root),
                             'result_ref': copy.deepcopy(manifest.get('inner_result_ref')),
                             'metadata_ref': {k: a.references.get(label + '.metadata', {}).get(k) for k in ('path', 'sha256')},
                             'environment_ref': copy.deepcopy(env_ref),
                             'raw_ref': copy.deepcopy(raw_ref), 'raw_pid_scope': scope}
    return scope


def _audit_nested_receipts(a, manifest, request, envelope, child, raw, binding, event_ref, config, root, ready):
    label = 'topology_namespace'
    a.equal(sorted(request), sorted({'config', 'config_sha256', 'output_root', 'source_manifest',
                                     'outer', 'options', 'bridge_id'}), label + '.request.keys')
    a.equal(sorted(envelope), sorted({'inner_result_ref', 'source_before_ref', 'source_after_ref',
                                      'inner_lease_scope', 'worker_bindings', 'raw_ref'}), label + '.envelope.keys')
    for key, expected in [('config', config), ('config_sha256', _hash(config)),
                          ('output_root', str(root)), ('source_manifest', config.get('source_manifest')),
                          ('outer', manifest.get('outer_controller')), ('bridge_id', manifest.get('bridge_id'))]:
        a.equal(request.get(key), expected, label + '.request.' + key)
    options = a.obj(request.get('options'), label + '.request.options')
    for key in ('timeout_sec', 'term_grace_sec', 'kill_wait_sec', 'startup_timeout_sec'):
        a.check(type(options.get(key)) in (int, float) and options[key] > 0, 'contract_gap',
                label + '.request.options.' + key, 'positive finite timeout required')
    for key in ('inner_result_ref', 'source_before_ref', 'source_after_ref', 'raw_ref'):
        a.equal(envelope.get(key), manifest.get(key), label + '.envelope.' + key)
    # Child offsets are integers. Parent refs include immutable raw SHA/path.
    # Verify their exact translation independently, including raw semantics.
    leases = copy.deepcopy(manifest.get('inner_lease_scope'))
    if isinstance(leases, dict):
        leases['raw_event_ref'] = a.obj(leases.get('raw_event_ref'), label + '.lease_ref').get('event_index')
    a.equal(envelope.get('inner_lease_scope'), leases, label + '.envelope.inner_lease_scope')
    namespace = a.obj(ready.get('namespace'), label + '.ready.namespace')
    reported = a.obj(binding.get('reported_inner'), label + '.binding.reported_inner')
    a.equal(namespace.get('identity'), reported, label + '.ready.identity')
    a.equal(namespace.get('pid1_proc_scoped'), True, label + '.ready.pid1_proc_scoped')
    mounts = a.seq(namespace.get('proc_mounts'), label + '.ready.proc_mounts')
    active = [row for row in mounts if isinstance(row, dict) and row.get('mount_id') == namespace.get('active_proc_mount_id')]
    a.check(len(active) == 1 and active[0].get('mount') == '/proc' and active[0].get('root') == '/'
            and active[0].get('fs_type') == 'proc', 'contract_gap', label + '.ready.active_proc',
            'one active PIDNS-scoped proc declaration required; this is not kernel reauthentication')
    a.equal(ready.get('execution_evidence'), True, label + '.ready.execution_evidence')
    a.equal(ready.get('config_sha256'), manifest.get('config_sha256'), label + '.ready.config_sha256')
    network = a.obj(namespace.get('network'), label + '.ready.network')
    outer = a.obj(manifest.get('outer_controller'), label + '.outer_controller')
    for key, expected in [('isolated', True), ('euid', 0), ('uid_map', [[0, 1000, 1]]),
                          ('host_uid', 1000), ('host_cap_eff', 0), ('launcher_pid', 1),
                          ('host_net_namespace', outer.get('namespaces', {}).get('net')),
                          ('host_user_namespace', outer.get('namespaces', {}).get('user')),
                          ('cap_eff', reported.get('cap_eff')), ('interfaces', ['lo'])]:
        a.equal(network.get(key), expected, label + '.ready.network.' + key)
    for name in ('net', 'user'):
        for key in (name + '_namespace', 'observed_' + name + '_namespace'):
            a.equal(network.get(key), reported.get('namespaces', {}).get(name), label + '.ready.network.' + key)
    flags = network.get('loopback_flags')
    a.check(type(flags) is int and flags & 9 == 9, 'contract_gap', label + '.ready.loopback_flags', 'UP loopback flags required')
    ready_rows = [e for e in raw if e.get('event') == 'topology_namespace_ready']
    a.equal(ready_rows, [ready], label + '.ready.unique')
    locks = [e for e in raw if e.get('event') == 'isolation_locks_acquired']
    a.check(len(locks) == 1 and locks[0]['event_index'] < ready.get('event_index', -1),
            'nested_chronology', label + '.ready.chronology', 'locks before nested readiness required')
    workers = a.seq(manifest.get('worker_bindings'), label + '.worker_bindings')
    metadata = a.obj(child.get('execution_metadata'), label + '.child.execution_metadata')
    actual = a.seq(metadata.get('workers'), label + '.child.execution_metadata.workers')
    a.check(bool(workers) and len(workers) == len(actual), 'contract_gap', label + '.worker_count', 'complete worker generation bindings required')
    offsets, identities, translated = [], set(), []
    keys = ('logical_id', 'generation', 'pid', 'process_start_ticks', 'endpoint_generation', 'endpoint_gid')
    for index, value in enumerate(workers):
        field = label + '.worker_bindings[' + str(index) + ']'
        worker = a.obj(value, field)
        row = event_ref(worker.get('raw_event_ref'), field + '.raw_event_ref', 'topology_namespace_worker_bound')
        offset = row.get('event_index')
        offsets.append(offset)
        a.equal(row.get('binding'), {k: v for k, v in worker.items() if k != 'raw_event_ref'}, field + '.raw_binding')
        a.equal(row.get('execution_evidence'), True, field + '.execution_evidence')
        a.equal(worker.get('namespaces'), reported.get('namespaces'), field + '.namespaces')
        for key in ('host_pid', 'outer_pid'):
            a.check(key in worker and worker[key] is None, 'unknown_ancestor_mapping', field + '.' + key,
                    'no independent ancestor worker mapping in this schema; require explicit null')
        a.equal(worker.get('mapping_status'), 'nested_kernel_bound; ancestor mapping unobserved unless independently sampled', field + '.mapping_status')
        for key in ('generation', 'pid', 'process_start_ticks'):
            a.positive(worker.get(key), field + '.' + key)
        a.check(type(worker.get('pid')) is int and worker['pid'] > 1, 'invalid_descendant', field + '.pid', 'worker cannot be controller PID1')
        for key in ('logical_id', 'endpoint_gid', 'endpoint_generation'):
            a.check(isinstance(worker.get(key), str) and bool(worker[key]), 'contract_gap', field + '.' + key, 'nonempty identity required')
        identity = _encoded([worker.get(key) for key in keys])
        a.check(identity not in identities, 'duplicate_worker', field, 'duplicate worker binding')
        identities.add(identity)
        matches = [w for w in actual if isinstance(w, dict) and all(_equal(w.get(k), worker.get(k)) for k in keys)]
        a.check(len(matches) == 1, 'worker_lineage', field, 'must identify exactly one child metadata worker generation')
        created = [e for e in raw if e.get('event') == 'endpoint_created' and all(_equal(e.get(k), worker.get(k)) for k in keys)]
        starts = [e for e in raw if e.get('event') == 'process_started' and all(_equal(e.get(k), worker.get(k)) for k in keys if k not in ('endpoint_gid', 'endpoint_generation', 'process_start_ticks'))]
        exits = [e for e in raw if e.get('event') == 'process_exit' and all(_equal(e.get(k), worker.get(k)) for k in keys)]
        a.check(len(created) == len(starts) == len(exits) == 1 and type(offset) is int
                and ready.get('event_index', -1) < starts[0]['event_index'] < created[0]['event_index'] < offset < exits[0]['event_index'],
                'worker_lineage', field + '.chronology', 'ready -> start -> endpoint -> binding -> exit required')
        translated.append(dict(worker, raw_event_ref=offset))
    a.equal(envelope.get('worker_bindings'), translated, label + '.envelope.worker_bindings')
    a.equal([e.get('event_index') for e in raw if e.get('event') == 'topology_namespace_worker_bound'], offsets,
            label + '.worker_raw_coverage')
    a.equal(len([e for e in raw if e.get('event') == 'endpoint_created']), len(workers), label + '.endpoint_coverage')
    stamps = [e.get('monotonic_ns') for e in raw]
    a.check(all(type(t) is int and t > 0 for t in stamps), 'nested_chronology', label + '.raw',
            'raw timestamps required; buffered worker clocks need not follow global file order')
    cleanup_rows = [e for e in raw if e.get('event') == 'topology_cleanup']
    end_rows = [e for e in raw if e.get('event') == 'runtime_end']
    a.check(len(cleanup_rows) == len(end_rows) == 1
            and all(type(e.get('monotonic_ns')) is int for e in [ready, *cleanup_rows, *end_rows])
            and ready['event_index'] < cleanup_rows[0]['event_index'] < end_rows[0]['event_index']
            and ready['monotonic_ns'] <= cleanup_rows[0]['monotonic_ns'] <= end_rows[0]['monotonic_ns'],
            'nested_chronology', label + '.cleanup.chronology', 'ready -> canonical cleanup -> runtime end required')
    if len(cleanup_rows) == 1:
        a.equal(cleanup_rows[0].get('receipt'), child.get('cleanup_receipt'), label + '.cleanup.canonical_receipt')
        for key in ('survivors', 'errors'):
            a.check(child.get('cleanup_receipt', {}).get(key) in (None, []), 'cleanup_unconfirmed',
                    label + '.cleanup.' + key, 'no cleanup survivors/errors allowed')
        a.equal(child.get('cleanup_receipt', {}).get('closed'), True, label + '.cleanup.closed')
    for key in ('execution', 'execution_metadata'):
        value = child.get(key)
        if isinstance(value, dict):
            for item in [value, *a.seq(value.get('workers', []), label + '.' + key + '.workers')]:
                item = a.obj(item, label + '.' + key + '.worker')
                a.check('pid_namespace' not in item and 'pid_interpretation' not in item,
                        'recursive_wrapper', label + '.' + key, 'child must not contain wrapper annotations')
                for mapping in ('outer_pid', 'host_pid'):
                    a.check(item.get(mapping) is None, 'unknown_ancestor_mapping', label + '.' + key + '.' + mapping,
                            'ancestor mapping is unobserved')


def _audit_nested_command(a, manifest, request):
    """Audit the serialized fixed launch, never execute it or trust its flags."""
    label = 'topology_namespace.command'
    command = a.seq(manifest.get('command'), label)
    if not a.check(len(command) == 25 and all(isinstance(x, str) and x and '\0' not in x for x in command),
                   'contract_gap', label, 'fixed namespace wrapper/child argv required'):
        return
    a.equal(command[:10], ['unshare', '--user', '--map-root-user', '--net', '--pid', '--fork',
                          '--mount', '--mount-proc', '--kill-child=SIGKILL', '--'], label + '.wrapper')
    a.equal(command[11:15], ['-B', '-m', 'r2f.runtime.discovery_network', '_enter'], label + '.entry')
    a.equal(command[16], '--', label + '.separator')
    a.equal(command[18:22], ['-B', '-m', 'r2f.runtime.topology_namespace', '_child'], label + '.child')
    a.equal(command[10], command[17], label + '.python')
    a.check(Path(command[10]).is_absolute(), 'invalid_path', label + '.python', 'absolute python path required')
    ref = a.obj(manifest.get('request_ref'), label + '.request_ref')
    a.equal(command[22], ref.get('path'), label + '.request_path')
    a.equal(command[23], ref.get('sha256'), label + '.request_sha256')
    a.check(command[24].isdigit() and int(command[24]) > 2, 'contract_gap', label + '.control_fd', 'child control FD required')
    caller = a.obj(_decode(command[15]), label + '.caller')
    outer = a.obj(request.get('outer'), label + '.outer')
    a.equal(caller, {'host_uid': outer.get('uid'), 'host_cap_eff': outer.get('cap_eff'),
                    'launcher_pid': outer.get('pid'),
                    'host_net_namespace': outer.get('namespaces', {}).get('net'),
                    'host_user_namespace': outer.get('namespaces', {}).get('user')}, label + '.caller')
