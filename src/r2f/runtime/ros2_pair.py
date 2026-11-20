"""Real public-API ROS2 pair, isolated into two independently initialized processes.

Contract: run_pair(config: dict, output_dir: pathlib.Path) -> dict.
No fixture path exists. Failed preflight has real_execution=False, execution=None.
The runtime records facts, not oracle verdicts or campaign lifecycle decisions.
"""
from __future__ import annotations
import hashlib
import json
import math
import os
from pathlib import Path
import re
import shutil
import time
import uuid

from .domains import IsolationError, LeaseSet
from .environment import REPO, RMW_NAMES, activated_environment, check_environment
from .process import WorkerPool

RECEIPT_KEYS = ('planned', 'submitted', 'carrier_admitted', 'configured', 'applied', 'hit', 'effect_observed')

#: Roles with an explicit RMW identity contract.  reader2/reader3 are the
#: recreation roles used by the recovery target; writer2/probe are aliases that
#: inherit the writer vendor and never change the mixed-vendor decision.
EFFECTIVE_RMW_ROLES = ('writer', 'reader', 'reader2', 'reader3')


def _role_alias(role):
    if role in ('writer', 'writer2', 'probe'):
        return 'writer'
    if role in ('reader', 'reader2', 'reader3'):
        # reader2/reader3 have their own (possibly explicit) vendor rows and
        # only fall back to reader during effective_rmws resolution.
        return role
    if role.startswith('reader') and role[6:].isdigit():
        return 'reader'
    return role


def effective_rmws(config):
    """Resolve the per-role vendor map without inventing a third RMW.

    Absent ``rmws`` preserves the historical single-vendor contract exactly:
    every role resolves to ``config['rmw']``.  When the field is present, a
    missing writer/reader falls back to ``config['rmw']``; a missing reader2/3
    falls back to the effective reader vendor.  Values are validated in
    ``normalize_config`` so this function is also usable on already normalized
    configs.
    """
    base = config.get('rmw', 'rmw_cyclonedds_cpp')
    raw = config.get('rmws')
    if raw is None:
        writer = reader = base
    else:
        _validate_rmws(raw)
        writer = raw.get('writer', base)
        reader = raw.get('reader', base)
    result = {'writer': writer, 'reader': reader,
              'reader2': raw.get('reader2', reader) if isinstance(raw, dict) else reader,
              'reader3': raw.get('reader3', reader) if isinstance(raw, dict) else reader}
    return result


def role_rmw(config, role):
    """Vendor for any WorkerPool role, including writer2/probe/reader4..9."""
    values = effective_rmws(config)
    alias = _role_alias(role)
    if alias in values:
        return values[alias]
    return config.get('rmw', 'rmw_cyclonedds_cpp')


def is_mixed_rmws(config):
    return len(set(effective_rmws(config).values())) > 1


def pair_is_mixed(config):
    values = effective_rmws(config)
    return values['writer'] != values['reader']


def role_rmw_differs_from_config(config):
    """True when any explicit role vendor differs from the global default."""
    return any(role_rmw(config, role) != config.get('rmw') for role in EFFECTIVE_RMW_ROLES)


def observation_uses_single_vendor_contract(config):
    """The passive sidecar only knows the writer/reader pair vendors."""
    return pair_is_mixed(config)


def mixed_base_environment(environment):
    """Remove single-vendor profile/transport selectors before role overlays.

    A base environment that still names one vendor's XML or QoS override would
    apply to every role, including the other vendor.  The per-role overlays are
    the only authority once any role overrides the global vendor.
    """
    env = dict(environment)
    for key in ('CYCLONEDDS_URI', 'FASTRTPS_DEFAULT_PROFILES_FILE',
                'FASTDDS_DEFAULT_PROFILES_FILE', 'RMW_FASTRTPS_USE_QOS_FROM_XML',
                'ROS_DISCOVERY_SERVER'):
        env.pop(key, None)
    return env


def role_rmw_environments(config, role_environments=None):
    """Inject per-role RMW_IMPLEMENTATION through the existing role_env path.

    When ``rmws`` is absent the input mapping is returned unchanged.  This keeps
    the default environment byte-for-byte compatible with the single-vendor
    path, including dict key insertion order.
    """
    environments = {role: dict(env) for role, env in (role_environments or {}).items()}
    if 'rmws' not in config:
        return environments
    for role in ('writer', 'writer2', 'probe', 'reader', 'reader2', 'reader3',
                 'reader4', 'reader5', 'reader6', 'reader7', 'reader8', 'reader9'):
        environments.setdefault(role, {})['RMW_IMPLEMENTATION'] = role_rmw(config, role)
    return environments


def mixed_worker_start_failure(config, reason):
    """Name a per-vendor worker startup failure as blocked, never a retry.

    A worker exiting before any endpoint exists is the only observable a
    load/initialization failure leaves.  For an explicit ``rmws`` map this is
    surfaced with a stable ``blocked:`` prefix so it cannot be mistaken for a
    SUT failure or a healthy single-vendor fallback.
    """
    if ('rmws' in config and isinstance(reason, str)
            and (reason.startswith('worker_exited')
                 or reason.startswith('worker_cleanup_error:worker_exited'))):
        return 'blocked:mixed_vendor_worker_start_failed:' + reason
    return reason


def require_role_rmw(config, role, token):
    """Fail closed with an explicit role/expected/observed reason."""
    expected = role_rmw(config, role)
    observed = token.get('rmw_identifier') if isinstance(token, dict) else None
    if observed != expected:
        raise RuntimeError('rmw_identity_mismatch:' + role + ':expected=' + expected
                           + ':observed=' + str(observed))
    return expected


def require_rmws_available(environment, config):
    """Fail closed when a configured vendor library is known unavailable.

    ``check_environment`` probes both reviewed RMW libraries.  A missing reader
    vendor is a blocked capability, never a silent fallback to the writer's
    vendor.  Test stubs without the probe keep their historical behaviour.
    """
    probed = environment.get('rmw') if isinstance(environment, dict) else None
    if not isinstance(probed, dict) or not probed:
        return
    for role, rmw in effective_rmws(config).items():
        record = probed.get(rmw)
        if isinstance(record, dict) and record.get('available') is False:
            raise RuntimeError('blocked:rmw_unavailable:' + role + ':' + rmw)


def observed_rmws_from_events(events):
    observed = {}
    for role in EFFECTIVE_RMW_ROLES:
        token = next((event for event in events
                      if event.get('event') == 'endpoint_created' and event.get('role') == role), None)
        if token is not None:
            observed[role] = token
    return observed


def effective_rmws_receipt(config, observed=None):
    """Raw/result receipt: per-role configured RMW and observed identifier."""
    expected = effective_rmws(config)
    observed = observed or {}
    roles = {}
    for role in EFFECTIVE_RMW_ROLES:
        token = observed.get(role)
        if isinstance(token, dict):
            identifier = token.get('rmw_identifier')
        elif isinstance(token, str):
            identifier = token
        else:
            identifier = None
        roles[role] = {'rmw': expected[role],
                       'observed_rmw_identifier': identifier,
                       'match': None if identifier is None else identifier == expected[role]}
    return {'schema': 'r2f.effective-rmws/v1',
            'configured': 'rmws' in config,
            'mixed': len(set(expected.values())) > 1,
            'roles': roles}


class _MixedObservationContext:
    """Explicit absence sidecar for mixed-vendor runs.

    ``instrumentation_bridge.prepare`` derives one vendor profile policy from
    ``config['rmw']`` and its capture identity requires every worker to report
    that same RMW.  Claiming it observed a mixed pair would be false.  This
    context keeps the base environment clean and reports the limitation as an
    unknown observation, never as a SUT defect or a failed run.
    """

    def __init__(self, config, base_env):
        from r2f.instrumentation.cyclonedds import environment_for_mode
        self.config = dict(config)
        self.base_env = mixed_base_environment(environment_for_mode(base_env, mode='blackbox'))
        self.role_env = role_rmw_environments(config, {})

    def capture_worker(self, role, pid, endpoint_event):
        return {'accepted': False, 'reason': 'mixed_vendor_passive_sidecar_unsupported'}

    def capture_graph(self, graph, ack):
        return {'accepted': False, 'reason': 'mixed_vendor_passive_sidecar_unsupported'}

    def collect(self, result):
        return {'schema': 'r2f.runtime-instrumentation/v1',
                'observation_mode': 'blackbox',
                'status': 'incomplete', 'verdict': 'unknown',
                'reason': 'mixed_vendor_passive_sidecar_unsupported',
                'intake': [], 'candidate_count': 0,
                'execution_metadata_only': True}


class RoleRmwWorkerPool(WorkerPool):
    """Apply the role vendor through WorkerPool while preserving process.py.

    WorkerPool.start currently writes RMW_IMPLEMENTATION from config['rmw']
    after role_environments.  Passing a per-role config view during that one
    call makes the existing global write role-correct without changing the
    process ownership or lifecycle code.  The role_env still carries the same
    RMW_IMPLEMENTATION for audit and for any future reordering.
    """

    def start(self, role):
        original = self.config
        per_role = role_rmw(original, role)
        if per_role == original.get('rmw'):
            return super().start(role)
        self.config = {**original, 'rmw': per_role}
        try:
            return super().start(role)
        finally:
            self.config = original


EXPLORATORY_TARGETS = {
    'discovery.late_endpoint.fuzz_v1': {
        'runtime_capability': 'public_endpoint_pair_fuzz',
        'carrier': 'ros2_endpoint',
        'mutation_kind': 'late_joiner',
        'backend': 'endpoint_pair',
        'surfaces': ('P01', 'W01', 'W02'),
    },
    'discovery.late_join_delivery.fuzz_v1': {
        'runtime_capability': 'public_endpoint_pair_fuzz',
        'carrier': 'ros2_endpoint',
        'mutation_kind': 'late_joiner',
        'backend': 'endpoint_pair',
        'surfaces': ('P01', 'W01', 'W02'),
    },
    'builtin.discovery_projection_reconciliation.fuzz_v1': {
        'runtime_capability': 'public_qos_projection_fuzz',
        'carrier': 'ros2_endpoint',
        'mutation_kind': 'endpoint_qos_variation',
        'backend': 'projection_pair',
        'surfaces': ('P02', 'P03', 'P04', 'P05', 'P06'),
    },
    'discovery.interruption_recovery.fuzz_v1': {
        'runtime_capability': 'discovery_drop_window_chain',
        'carrier': 'wire_proxy',
        'mutation_kind': 'discovery_drop',
        'backend': 'discovery_namespace',
        'surfaces': ('P01', 'E01', 'E02', 'E03', 'W01', 'W02'),
    },
    'discovery.interruption_recovery.fuzz_windows_v1': {
        'runtime_capability': 'f2_windows_v1',
        'carrier': 'wire_proxy', 'mutation_kind': 'discovery_drop',
        'backend': 'discovery_namespace', 'surfaces': ('E02', 'E03'),
    },
    'discovery.match_compatibility.fuzz_v1': {
        'runtime_capability': 'pair_relation_guard',
        'carrier': 'ros2_endpoint',
        'mutation_kind': 'endpoint_qos_variation',
        'backend': 'relation_pair',
        'surfaces': ('G01',),
    },
}

# surface -> (binding field, config path, evidence class)
EXPLORATORY_BINDINGS = {
    'P01': ('reader_delay_sec', ('reader_delay_sec',), 'timing'),
    'P02': ('qos.reliability', ('qos', 'reliability'), 'qos'),
    'P03': ('qos.durability', ('qos', 'durability'), 'qos'),
    'P04': ('qos.deadline_ns', ('qos', 'deadline_ns'), 'qos'),
    'P05': ('qos.liveliness', ('qos', 'liveliness'), 'qos'),
    'P06': ('qos.liveliness_lease_duration_ns', ('qos', 'liveliness_lease_duration_ns'), 'qos_config_only'),
    # W01 is one surface over three concrete inputs; each is bound by name.
    'W01': ('qos.depth', ('qos', 'depth'), 'count'),
    'W02': ('publish_interval_sec', ('publish_interval_sec',), 'timing'),
    # E01 is single-window: the legacy top-level value is the one the runtime
    # uses. Multi-window E02 keeps the array path and is still capability pending.
    'E01': ('drop_duration_sec', ('drop_duration_sec',), 'environment'),
    'E02': ('environment_windows.count', ('environment_windows',), 'environment_count'),
    'E03': ('environment_windows.anchor_offset_sec',
            ('environment_windows', 0, 'trigger_offset_sec'), 'environment_anchor'),
    # G01 is a pair relation, not a single scalar: both role policies must be
    # submitted and both must survive normalization before any relation claim.
    'G01': ('writer_qos.reliability,reader_qos.reliability', None, 'relation_pair'),
}
W01_EXTRA_FIELDS = ('pre_match_count', 'post_match_count')

#: F2 recovery fuzz admits the environment surface family E01/E02/E03 with a
#: 1..3 window chain.  Admission checks only input shape and declared surfaces;
#: it is not execution evidence and cannot turn a planned chain into a hit.
#: G01 and every other surface stay outside this target's admission.
RECOVERY_FUZZ_TARGET = 'discovery.interruption_recovery.fuzz_v1'
RECOVERY_FUZZ_ADMITTED_SURFACES = frozenset({'E01', 'E02', 'E03'})
MAX_RECOVERY_WINDOWS = 3


F2_WINDOWS_TARGET = 'discovery.interruption_recovery.fuzz_windows_v1'
F2_ANCHOR_POLICY = 'f2_anchor_not_before_v1'


def f2_windows_operations(config):
    """Input-only binding gate. Never supplies runtime observations or hits."""
    from r2f.core.evidence import canonical_hash
    if config.get('target_id') != F2_WINDOWS_TARGET:
        return []
    if (config.get('runtime_capability') != 'f2_windows_v1'
            or type(config.get('target_revision')) is not int or config['target_revision'] != 0
            or config.get('anchor_timing_policy') != F2_ANCHOR_POLICY
            or type(config.get('anchor_lateness_tolerance_sec')) not in (int, float)
            or config['anchor_lateness_tolerance_sec'] != 0.5):
        raise ValueError('f2_windows_explicit_capability_and_timing_policy_required')
    if config.get('arm') != 'mutated' or config.get('environment_mutation') is not None:
        raise ValueError('f2_windows_requires_mutated_discovery_drop')
    plan = config.get('mutation_plan')
    if not isinstance(plan, dict) or canonical_hash(plan) != config.get('mutation_plan_sha256'):
        raise ValueError('f2_windows_plan_hash_mismatch')
    final = plan.get('final_config')
    if not isinstance(final, dict) or config.get('final_input') != final:
        raise ValueError('f2_windows_final_input_mismatch')
    for field in ('environment_windows', 'runtime_capability', 'anchor_timing_policy', 'anchor_lateness_tolerance_sec'):
        if field not in final or final[field] != config.get(field):
            raise ValueError('f2_windows_final_input_mismatch:' + field)
    from r2f.core.mutation_plan import MutationPlan
    MutationPlan.from_dict(plan).validate()
    operations = plan.get('operations')
    # Initial activation is explicitly single-surface/single-operation.
    if not isinstance(operations, list) or len(operations) != 1:
        raise ValueError('f2_windows_requires_one_operation')
    operation = operations[0]
    if (not isinstance(operation, dict) or not isinstance(operation.get('op_id'), str)
            or not operation['op_id'] or (operation.get('surface_id'), operation.get('mutator_id'))
                not in (('E02', 'M07'), ('E03', 'M06'))
            or config.get('fuzz_surfaces') != [operation['surface_id']]):
        raise ValueError('f2_windows_operation_binding_mismatch')
    windows = config.get('environment_windows')
    from .discovery_network import validate_window_plan
    validate_window_plan(windows)
    parameters = operation.get('parameters', {})
    if (operation.get('applicability_result', {}).get('status') != 'needs_observation'
            or any(parameters.get(k, False) is not False for k in ('anchor_observed', 'post_match_observed'))):
        raise ValueError('f2_windows_runtime_guard_not_pending')
    if plan.get('runtime_windows') != [dict(schema='r2f.runtime-window/v1', **w) for w in windows]:
        raise ValueError('f2_windows_plan_window_mismatch')
    if any(config.get(k) for k in ('reader_recreate_in_window', 'writer_churn_in_window',
                                   'publish_burst_in_window', 'discovery_probe')):
        raise ValueError('f2_windows_additional_actions_not_enabled')
    if operation['surface_id'] == 'E02':
        submitted = config.get('submitted_parameters', {})
        if (type(parameters.get('window_count')) is not int
                or parameters.get('window_count') != len(windows)
                or parameters.get('restore_interval_sec') != submitted.get('environment_windows.restore_interval_sec')
                or submitted.get('environment_windows.count') != len(windows)
                or any(w['restore_interval_sec'] != submitted.get('environment_windows.restore_interval_sec')
                       for w in windows)):
            raise ValueError('f2_windows_E02_count_or_interval_mismatch')
        if (parameters.get('anchor') != windows[0]['trigger_anchor']
                or parameters.get('anchor_offset_sec') != windows[0]['trigger_offset_sec']):
            raise ValueError('f2_windows_E02_anchor_or_offset_mismatch')
        affected = list(range(len(windows)))
    else:
        submitted = config.get('submitted_parameters', {})
        anchor = parameters.get('anchor')
        affected = [i for i, w in enumerate(windows) if w['trigger_anchor'] == anchor]
        if (anchor not in ('fresh_match', 'previous_restore_confirmed') or not affected
                or type(parameters.get('offset_sec')) not in (int, float)
                or any(windows[i]['trigger_offset_sec'] != parameters['offset_sec'] for i in affected)
                or submitted.get('environment_windows.anchor') != anchor
                or submitted.get('environment_windows.anchor_offset_sec') != parameters['offset_sec']):
            raise ValueError('f2_windows_E03_anchor_or_offset_mismatch')
    return [dict(operation, affected_window_indices=affected)]


def recovery_fuzz_admission_error(config):
    """Return an explicit non-admission reason, or None for the admitted slice.

    This is an input-admission gate, not execution evidence.  It never claims a
    run happened; it only refuses configs whose declared surfaces/window shape
    are outside the target's reviewed contract.  The target id is never
    rewritten, so a config that mixes another target's surface (e.g. G01) is
    refused by the binding check or here.
    """
    if config.get('target_id') == F2_WINDOWS_TARGET:
        try:
            f2_windows_operations(config)
        except (ValueError, TypeError, KeyError) as exc:
            return 'unsupported_capability:' + str(exc)
        return None
    if config.get('target_id') != RECOVERY_FUZZ_TARGET:
        return None
    surfaces = set(config.get('fuzz_surfaces') or ())
    pending = sorted(surfaces - RECOVERY_FUZZ_ADMITTED_SURFACES)
    if pending:
        return 'unsupported_capability:surface_not_admitted:' + ','.join(pending)
    windows = config.get('environment_windows')
    if windows is not None:
        if not isinstance(windows, (list, tuple)):
            return 'unsupported_capability:environment_windows_not_a_list'
        if not 1 <= len(windows) <= MAX_RECOVERY_WINDOWS:
            return ('unsupported_capability:window_count_not_admitted:'
                    + str(len(windows)))
        # Admission is shape-only; the window contract itself is validated by
        # the namespace normalizer before any carrier action.  Reject an
        # obviously invalid first anchor here so admission never returns None
        # for a shape the runtime must refuse.
        first = windows[0]
        if isinstance(first, dict) and first.get('trigger_anchor') not in (None, 'fresh_match'):
            return 'unsupported_capability:first_window_trigger_anchor_not_fresh_match'
    return None


def _get_path(config, path):
    value = config
    for key in path:
        if isinstance(key, int):
            if not isinstance(value, list) or not -len(value) <= key < len(value):
                return None, False
        elif not isinstance(value, dict) or key not in value:
            return None, False
        value = value[key]
    return value, True


def exploratory_bindings(config):
    """Return declared binding expectations for an exploratory input.

    Inputs must declare every surface they claim to exercise together with the
    exact submitted value. Missing declarations are an adapter error; they are
    never treated as a no-op.
    """
    surfaces = config.get('fuzz_surfaces')
    if not isinstance(surfaces, list) or not surfaces:
        raise ValueError('exploratory_input_requires_fuzz_surfaces')
    submitted = config.get('submitted_parameters')
    if not isinstance(submitted, dict):
        raise ValueError('exploratory_input_requires_submitted_parameters')
    target = EXPLORATORY_TARGETS.get(config.get('target_id'))
    if target is None:
        raise ValueError('unknown_exploratory_target')
    allowed = set(target['surfaces'])
    unknown = [sid for sid in surfaces if sid not in allowed]
    if unknown:
        raise ValueError('surface_not_supported_by_target:' + ','.join(sorted(set(unknown))))
    expected = []
    for surface_id in surfaces:
        field, path, evidence_class = EXPLORATORY_BINDINGS[surface_id]
        if surface_id == 'E03' and config.get('target_id') == F2_WINDOWS_TARGET:
            operation = f2_windows_operations(config)[0]
            for index in operation['affected_window_indices']:
                for label, name in (('anchor', 'trigger_anchor'), ('anchor_offset_sec', 'trigger_offset_sec')):
                    key = 'environment_windows.' + label
                    expected.append({'surface_id': surface_id, 'field': key,
                        'path': ['environment_windows', index, name], 'submitted': submitted[key],
                        'configured': config['environment_windows'][index][name],
                        'evidence_class': evidence_class, 'affected_window_indices': operation['affected_window_indices']})
            continue
        if surface_id == 'G01':
            pair_fields = ('writer_qos.reliability', 'reader_qos.reliability')
            for pair_field, role in zip(pair_fields, ('writer', 'reader')):
                if pair_field not in submitted:
                    raise ValueError('submitted_parameter_missing:' + pair_field)
                role_qos = config.get(role + '_qos')
                if not isinstance(role_qos, dict) or 'reliability' not in role_qos:
                    raise ValueError('binding_field_missing:' + pair_field)
                expected.append({'surface_id': surface_id, 'field': pair_field,
                                 'path': [role + '_qos', 'reliability'],
                                 'submitted': submitted[pair_field],
                                 'configured': role_qos['reliability'],
                                 'evidence_class': evidence_class})
            continue
        if field not in submitted:
            raise ValueError('submitted_parameter_missing:' + field)
        value, present = _get_path(config, path)
        if not present:
            raise ValueError('binding_field_missing:' + field)
        expected.append({'surface_id': surface_id, 'field': field, 'path': list(path),
                         'submitted': submitted[field], 'configured': value,
                         'evidence_class': evidence_class})
        if surface_id == 'W01':
            for extra in W01_EXTRA_FIELDS:
                if extra not in submitted or extra not in config:
                    raise ValueError('binding_field_missing:' + extra)
                expected.append({'surface_id': surface_id, 'field': extra, 'path': [extra],
                                 'submitted': submitted[extra], 'configured': config[extra],
                                 'evidence_class': evidence_class})
        if surface_id == 'E03':
            if submitted.get('environment_windows.anchor_offset_sec') is None:
                raise ValueError('binding_field_missing:environment_windows.anchor_offset_sec')
        if surface_id == 'E02':
            declared = submitted['environment_windows.count']
            windows = config.get('environment_windows')
            if not isinstance(declared, int) or isinstance(declared, bool) or declared < 1:
                raise ValueError('invalid_environment_window_count')
            if not isinstance(windows, list) or len(windows) != declared:
                raise ValueError('environment_window_count_not_preserved')
            if 'environment_windows.restore_interval_sec' not in submitted:
                raise ValueError('binding_field_missing:environment_windows.restore_interval_sec')
            expected.append({'surface_id': surface_id, 'field': 'environment_windows.restore_interval_sec',
                             'path': ['environment_windows', 0, 'restore_interval_sec'],
                             'submitted': submitted['environment_windows.restore_interval_sec'],
                             'configured': windows[0].get('restore_interval_sec'),
                             'evidence_class': 'environment_interval'})
    return expected


def require_binding_preserved(expected, config):
    """V35: any silent normalization away from the submitted value fails closed."""
    mismatches = []
    for binding in expected:
        if binding['field'] == 'environment_windows.count':
            windows = config.get('environment_windows')
            value, present = (len(windows), True) if isinstance(windows, list) else (None, False)
        else:
            value, present = _get_path(config, binding['path'])
        if not present or value != binding['submitted']:
            mismatches.append({'surface_id': binding['surface_id'], 'field': binding['field'],
                               'submitted': binding['submitted'], 'effective': value if present else None})
    if mismatches:
        raise ValueError('exploratory_binding_normalized_away:' +
                         ','.join(item['field'] for item in mismatches))
    return mismatches


def exploratory_runtime_capabilities():
    return {target_id: {'implemented': (spec['backend'] not in ('unsupported_relation_observation', 'relation_pair')
                                        or target_id == 'discovery.match_compatibility.fuzz_v1'),
                        **({'runtime_implemented': True, 'runner_executable': True,
                            'target_revision': 1, 'live_qualified': False,
                            'activation_status': 'revision1_route_declared_executable_live_unverified'}
                           if target_id == 'discovery.match_compatibility.fuzz_v1' else {}),
                        'backend': spec['backend'],
                        'carrier': spec['carrier'],
                        'mutation_kind': spec['mutation_kind'],
                        **({'runtime_capability': spec['runtime_capability']}
                           if target_id == F2_WINDOWS_TARGET else {}),
                        # Window execution is a route capability, not activation
                        # of the generation-only E02/E03/G01 surfaces.
                        'multi_window': (target_id in (RECOVERY_FUZZ_TARGET, F2_WINDOWS_TARGET)
                                         and spec['backend'] == 'discovery_namespace'
                                         and spec['carrier'] == 'wire_proxy'
                                         and spec['mutation_kind'] == 'discovery_drop'),
                        'max_runtime_windows': (MAX_RECOVERY_WINDOWS
                                                if target_id in (RECOVERY_FUZZ_TARGET, F2_WINDOWS_TARGET) else 1),
                        'surfaces': list(spec['surfaces']),
                        'available': None if spec['backend'] != 'unsupported_relation_observation' else False,
                        'admitted': False,
                        'admission_requires_live_preflight': True,
                        'execution_evidence': False}
            for target_id, spec in EXPLORATORY_TARGETS.items()}


def _validate_rmws(value):
    """Strict schema for the optional mixed-vendor map.

    Only writer/reader/reader2/reader3 may be named, and every supplied value
    must be one of the two reviewed RMWs.  Missing keys are resolved later by
    ``effective_rmws``; they are never silently replaced with a third vendor.
    """
    if not isinstance(value, dict):
        raise ValueError('invalid_rmws:rmws:must_be_dict')
    unknown = set(value) - {'writer', 'reader', 'reader2', 'reader3'}
    if unknown:
        raise ValueError('unsupported_rmws_role:rmws.' + ','.join(sorted(unknown)))
    for role, rmw in value.items():
        if not isinstance(rmw, str) or not rmw:
            raise ValueError('unsupported_rmw:rmws.' + role + ':empty')
        if rmw not in RMW_NAMES:
            raise ValueError('unsupported_rmw:rmws.' + role + ':' + rmw)


def normalize_config(config):
    if not isinstance(config, dict):
        raise ValueError('config must be a dictionary')
    if any(k in config for k in ('fixture', 'fake_events', 'offline_events', 'events')):
        raise ValueError('offline_fixture_not_supported')
    c = dict(config)
    c.setdefault('run_id', uuid.uuid4().hex)
    c.setdefault('campaign_id', c['run_id'])
    c.setdefault('domain_id', c.get('domain', 173))
    if isinstance(c['domain_id'], bool) or not isinstance(c['domain_id'], int) or not 0 <= c['domain_id'] <= 231:
        raise ValueError('domain_id must be an integer in 0..231')
    if not isinstance(c['run_id'], str) or not re.fullmatch(r'[A-Za-z0-9_.-]{1,128}', c['run_id']):
        raise ValueError('run_id must contain 1..128 safe identifier characters')
    c.setdefault('rmw', 'rmw_cyclonedds_cpp')
    if not isinstance(c['rmw'], str) or c['rmw'] not in RMW_NAMES:
        raise ValueError('unsupported_rmw:rmw:' + str(c['rmw']))
    if 'rmws' in c:
        _validate_rmws(c['rmws'])
    c.setdefault('target_id', 'discovery.late_endpoint')
    c.setdefault('target_revision', 0)
    c.setdefault('arm', 'mutated')
    if c['arm'] not in ('baseline', 'transparent', 'mutated', 'negative'):
        raise ValueError('invalid_arm')
    c.setdefault('mutation_kind', 'late_joiner')
    c.setdefault('carrier', 'ros2_endpoint')
    c.setdefault('mutation_parameters', {})
    if not isinstance(c['mutation_parameters'], dict):
        raise ValueError('mutation_parameters must be a dict')
    params = c['mutation_parameters']
    c.setdefault('reader_delay_sec', params.get('reader_delay_sec', params.get('delay_sec', 0.15)))
    c.setdefault('baseline_reader_delay_sec', 0.02)
    for key, default in (('match_timeout_sec', 8), ('delivery_timeout_sec', 5), ('startup_timeout_sec', 10),
                         ('publish_interval_sec', 0.03)):
        c.setdefault(key, default)
    for key in ('reader_delay_sec', 'baseline_reader_delay_sec', 'match_timeout_sec', 'delivery_timeout_sec',
                'startup_timeout_sec', 'publish_interval_sec'):
        if isinstance(c[key], bool):
            raise ValueError('invalid_timeout:' + key)
        value = float(c[key])
        if not math.isfinite(value) or not 0 <= value <= 120:
            raise ValueError('invalid_timeout:' + key)
        c[key] = value
    c.setdefault('pre_match_count', c.get('pre_samples', 2))
    c.setdefault('post_match_count', c.get('post_samples', c.get('message_count', 6)))
    for key in ('pre_match_count', 'post_match_count'):
        if isinstance(c[key], bool) or not isinstance(c[key], int) or not 0 <= c[key] <= 10000:
            raise ValueError('invalid_count:' + key)
    if c['post_match_count'] < 3 or c['pre_match_count'] < 1:
        raise ValueError('need at least one prejoin and three postmatch samples')
    exploratory = c['target_id'] in EXPLORATORY_TARGETS
    default_qos = {'reliability': 'reliable', 'durability': 'volatile',
                   'depth': 64 if c['target_id'].startswith('builtin.discovery_projection_reconciliation') else 32}
    c['qos'] = {**default_qos, **c.get('qos', {})}
    for role in ('writer', 'reader'):
        c[role + '_qos'] = {**c['qos'], **c.get(role + '_qos', {})}
    # QoS schema is a per-target capability, not a global widening: the legacy
    # projection target already required the extended public fields, the new
    # exploratory QoS targets need them too, and the late-join / interruption
    # targets stay strictly limited to reliability/durability/depth.
    projection_schema = c['target_id'].startswith('builtin.discovery_projection_reconciliation')
    extended_qos = exploratory or projection_schema
    if projection_schema:
        default_qos = {**default_qos, 'deadline_ns': INFINITE_QOS_NS,
                       'liveliness': 'automatic',
                       'liveliness_lease_duration_ns': INFINITE_QOS_NS}
        c['qos'] = {**default_qos, **{k: v for k, v in c.get('qos', {}).items()}}
    allowed_qos = ({'reliability', 'durability', 'depth', 'deadline_ns', 'liveliness',
                    'liveliness_lease_duration_ns', 'history'} if extended_qos
                   else {'reliability', 'durability', 'depth'})
    for label, q in (('qos', c['qos']), ('writer_qos', c['writer_qos']), ('reader_qos', c['reader_qos'])):
        if not isinstance(q, dict):
            raise ValueError('invalid_qos_mapping:' + label)
        unknown = set(q) - allowed_qos
        if unknown:
            raise ValueError('unsupported_qos_field:' + label + ':' + ','.join(sorted(unknown)))
        if q['reliability'] not in ('reliable', 'best_effort') or q['durability'] not in ('volatile', 'transient_local'):
            raise ValueError('unsupported_qos_policy')
        if isinstance(q['depth'], bool) or not isinstance(q['depth'], int) or not 1 <= q['depth'] <= 10000:
            raise ValueError('invalid_qos_depth')
        if extended_qos:
            _validate_exploratory_qos(q, label)
    fault_name = {'discovery.interruption_recovery': 'post_restore_delivery_omission',
                  'builtin.discovery_projection_reconciliation': 'controlled_projection_field_mismatch'}.get(c['target_id'], 'late_endpoint_delivery_omission')
    c.setdefault('fault_id', fault_name if c['arm'] == 'negative' else None)
    if c['fault_id'] not in (None, fault_name):
        raise ValueError('unsupported_fault_id')
    if c['fault_id'] and c['arm'] != 'negative':
        raise ValueError('fault_requires_negative_arm')
    c.setdefault('topic', '/r2f_' + c['run_id'].replace('-', '_').replace('.', '_'))
    if not re.fullmatch(r'/[A-Za-z_][A-Za-z0-9_/]*', c['topic']):
        raise ValueError('invalid_topic')
    c.setdefault('source_repo', str(REPO))
    c['source_repo'] = str(Path(c['source_repo']).resolve())
    c.setdefault('environment_mode', 'ros_overlay')
    c.setdefault('repeat', 0)
    for key in ('repeat', 'target_revision'):
        if isinstance(c[key], bool) or not isinstance(c[key], int) or c[key] < 0:
            raise ValueError('invalid_integer:' + key)
    c.setdefault('source_manifest', {})
    if c.get('fuzz_surfaces') is not None and not isinstance(c['fuzz_surfaces'], list):
        raise ValueError('fuzz_surfaces must be a list')
    if c.get('submitted_parameters') is not None and not isinstance(c['submitted_parameters'], dict):
        raise ValueError('submitted_parameters must be a dict')
    if c.get('environment_windows') is not None:
        from .discovery_network import validate_window_plan
        windows = validate_window_plan(c['environment_windows'])
        if c['target_id'] not in ('discovery.interruption_recovery',
                                  'discovery.interruption_recovery.fuzz_v1', F2_WINDOWS_TARGET):
            raise ValueError('environment_windows_only_supported_for_discovery_target')
        c['environment_windows'] = windows
    return c


INFINITE_QOS_NS = 9223372036854775807


def _validate_exploratory_qos(q, label):
    """Strict public-QoS contract for exploratory targets (no silent defaults)."""
    for key in ('deadline_ns', 'liveliness_lease_duration_ns'):
        if key in q:
            value = q[key]
            if type(value) is not int or not 0 <= value <= INFINITE_QOS_NS:
                raise ValueError('invalid_qos_duration:%s:%s' % (label, key))
    if 'liveliness' in q and q['liveliness'] not in ('automatic', 'manual_by_topic'):
        raise ValueError('unsupported_qos_liveliness:' + label)
    if 'history' in q and q['history'] != 'keep_last':
        raise ValueError('unsupported_qos_history:' + label)


def runtime_capabilities():
    """Implementation and dependency discovery are not live run admission."""
    tools = {name: shutil.which(name) is not None for name in ('unshare', 'ip', 'nft')}
    dependencies = all(tools.values())
    return {'public_endpoint_pair': True, 'independent_worker_processes': True,
            'transient_local_api': True, 'builtin_independent_projection': True,
            'projection_observation': {'implemented': True, 'available': None, 'admitted': False,
                'admission_requires_live_preflight': True, 'independent_sources': ['passive_rtps_wire', 'ros_graph']},
            'discovery_network': {
                'implemented': True, 'available': None if dependencies else False,
                'availability_status': 'requires_live_preflight' if dependencies else 'missing_dependency',
                'dependency_check': {'available': dependencies, 'tools': tools},
                'admitted': False, 'admission_requires_live_preflight': True,
                'implementation': 'isolated_netns_nft_discovery_udp_drop',
                'carrier': 'wire_proxy', 'target_id': 'discovery.interruption_recovery',
                'reason': 'implementation_present; namespace, locks, ports, observer and pair binding require this run admission',
                'execution_evidence': False}}


def wait_for_qos_binding(events, pool, role, endpoint_gid, timeout, emit=None, *, exactly_one):
    """Wait until the role's latest ``qos_binding_observed`` is GID-bound.

    ``pool.start(role)`` only returns once the worker acknowledges creation; the
    worker's following ``qos_binding_observed`` row may still be in flight.  This
    helper pumps the pool under a bounded deadline.  A timeout is an observation
    gap (tool/observer), never a SUT violation: it is recorded as
    ``discovery_observation_unknown`` before raising, and the original raw stays
    intact.  Identity checks are not relaxed: the latest row must belong to the
    same endpoint GID and must carry a non-empty observed profile.
    """
    def rows():
        return [e for e in events if e.get('event') == 'qos_binding_observed'
                and e.get('role') == role]

    try:
        pool.wait(lambda: bool(rows()), timeout, 'qos_binding_observation:' + role)
    except TimeoutError:
        if emit is not None:
            emit('discovery_observation_unknown',
                 reason='qos_binding_observation_timeout:' + role,
                 classification='observation_timeout_not_sut_violation',
                 role=role, budget_sec=timeout)
        raise RuntimeError('qos_binding_observation_missing:' + role)
    observed = rows()
    if exactly_one and len(observed) != 1:
        raise RuntimeError('qos_binding_observation_ambiguous:' + role)
    last = observed[-1]
    if last.get('observed_self_gid') != endpoint_gid:
        raise RuntimeError('qos_binding_observation_not_bound_to_endpoint:' + role)
    if last.get('observed_self') is None:
        raise RuntimeError('qos_binding_observation_empty:' + role)
    return last


def summarize_events(events):
    sends = [e for e in events if e['event'] == 'send' and e.get('phase') == 'post_match']
    recvs = [e for e in events if e['event'] == 'receive' and e.get('phase') == 'post_match']
    omitted = [e['send_sequence'] for e in events if e['event'] == 'harness_omission']
    sent, received = [e['send_sequence'] for e in sends], [e['send_sequence'] for e in recvs]
    contract_events = [e for e in events if e['event'] == 'delivery_contract']
    expected = contract_events[-1]['expected_sequences'] if contract_events else sorted(set(sent + omitted))
    intended_missing = sorted(set(expected) - set(received))
    missing = sorted(set(sent) - set(received))
    sentinel = expected[-1] if expected else None
    sentinel_received = sentinel in received if sentinel is not None else False
    writer = next((e for e in events if e['event'] == 'endpoint_created' and e.get('role') == 'writer'), {})
    reader = next((e for e in events if e['event'] == 'endpoint_created' and e.get('role') == 'reader'), {})
    writer_gid = writer.get('endpoint_gid')
    delivery_tokens = [e for e in events if e['event'] == 'receive']
    identity_complete = bool(writer_gid) and bool(delivery_tokens) and all(e.get('publisher_gid') for e in delivery_tokens)
    identity_consistent = all(e['publisher_gid'] == writer_gid for e in delivery_tokens) if identity_complete else None
    windows_open = [e for e in events if e['event'] == 'window_open' and e.get('window') == 'post_match_delivery']
    windows_close = [e for e in events if e['event'] == 'window_close' and e.get('window') == 'post_match_delivery']
    graph_stable = bool(writer_gid and reader.get('endpoint_gid') and windows_open and windows_close)
    graph_boundaries = {}
    if graph_stable:
        opening, closing = windows_open[-1]['monotonic_ns'], windows_close[-1]['monotonic_ns']
        for role in ('writer', 'reader'):
            graphs = sorted([e for e in events if e['event'] == 'graph_snapshot' and e.get('role') == role], key=lambda e: e['monotonic_ns'])
            before = [e for e in graphs if e['monotonic_ns'] <= opening]
            after = [e for e in graphs if e['monotonic_ns'] >= closing]
            bounded = ([before[-1]] if before else []) + [e for e in graphs if opening < e['monotonic_ns'] < closing] + ([after[0]] if after else [])
            valid = bool(before and after) and all(
                {p['endpoint_gid'] for p in g['publishers']} == {writer_gid}
                and {p['endpoint_gid'] for p in g['subscriptions']} == {reader['endpoint_gid']} for g in bounded)
            graph_stable = graph_stable and valid
            graph_boundaries[role] = {'before_index': before[-1].get('event_index') if before else None,
                                      'after_index': after[0].get('event_index') if after else None, 'stable': valid}
    correlated = bool(graph_stable and delivery_tokens and writer.get('pid') != reader.get('pid')
                      and all(e.get('payload_run_id') == writer.get('run_id') for e in delivery_tokens))
    match_kind = 'match_snapshot' if any(e['event'] == 'match_snapshot' for e in events) else 'match'
    matches = {role: any(e['event'] == match_kind and e.get('role') == role and e.get('matched_count') == 1
                        and (match_kind == 'match' or e.get('sampled_monotonic_ns', -1) >= reader.get('created_monotonic_ns', 0))
                        for e in events) for role in ('writer', 'reader')}
    return {'expected_post_match_sequences': expected, 'post_match_sent_sequences': sent,
            'post_match_received_sequences': received,
            'prejoin_sent_sequences': [e['send_sequence'] for e in events if e['event'] == 'send' and e.get('phase') == 'prejoin'],
            'prejoin_received_sequences': [e['send_sequence'] for e in events if e['event'] == 'receive' and e.get('phase') == 'prejoin'],
            'missing_sequences': missing, 'missing_expected_sequences': intended_missing,
            'harness_omitted_sequences': omitted, 'fault_control_primary_expected': 'fail' if omitted else None,
            'candidate_eligible': not bool(omitted), 'responsibility_layer': 'harness' if omitted else None,
            'sentinel_sequence': sentinel, 'sentinel_received': sentinel_received,
            'sut_missing_with_later_sentinel': [n for n in missing if sentinel_received and n < sentinel],
            'receive_order': received, 'in_order': received == sorted(received),
            'duplicate_sequences': sorted({x for x in received if received.count(x) > 1}),
            'writer_gid': writer_gid, 'reader_gid': reader.get('endpoint_gid'),
            'publisher_gid_evidence_complete': identity_complete,
            'publisher_gid_consistent': identity_consistent,
            'publisher_gid_status': 'verified' if identity_consistent is True else ('mismatch' if identity_consistent is False else 'unavailable_api'),
            'identity_basis': 'message_info_gid' if identity_complete else 'endpoint_correlated',
            'endpoint_identity_correlated': correlated, 'graph_stable_across_window': graph_stable,
            'graph_boundary_receipts': graph_boundaries,
            'identity_limitations': [] if identity_complete else ['Jazzy rclpy callback metadata has no publisher_gid; graph correlation is not direct per-sample DDS writer identity'],
            'writer_pid': writer.get('pid'), 'reader_pid': reader.get('pid'),
            'independent_processes': bool(writer.get('pid') and reader.get('pid') and writer['pid'] != reader['pid']),
            'reader_created_after_writer': bool(writer and reader and reader['created_monotonic_ns'] > writer['created_monotonic_ns']),
            'match_established': all(matches.values()), 'matched_roles': matches,
            'window_complete': any(e['event'] == 'window_close' and e.get('window') == 'post_match_delivery' for e in events),
            'observation_provenance': 'public_rclpy_graph_and_message_info',
            'independent_builtin_projection': False}


def wait_for_fresh_match(pool, events, reader_created_ns, timeout, emit=None):
    """Require command-correlated counts queried after the reader create return.

    Keep count-transition events as historical evidence. This live gate has no
    fallback to those events, including when an old transition says count=1.
    """
    deadline = time.monotonic() + timeout
    while True:
        requests = {}
        for role in ('writer', 'reader'):
            requested_ns = time.monotonic_ns()
            command_id = pool.command(role, 'snapshot')
            requests[role] = (command_id, requested_ns)
            if emit is not None:
                emit('match_snapshot_requested', role=role, command_id=command_id,
                     requested_monotonic_ns=requested_ns, reader_created_monotonic_ns=reader_created_ns)

        def snapshots():
            # A snapshot and its ACK can arrive in separate stdout chunks.
            # Only completed, unambiguous commands can release the live gate.
            responses = {role: {'match_snapshot': [], 'command_done': []} for role in requests}
            for index, event in enumerate(events):
                role, kind = event.get('role'), event.get('event')
                if role not in requests or kind not in responses[role]:
                    continue
                if (type(event.get('command_id')) is int
                        and event['command_id'] == requests[role][0]):
                    responses[role][kind].append((index, event))
            found = {}
            for role, response in responses.items():
                if len(response['match_snapshot']) != 1 or len(response['command_done']) != 1:
                    continue
                snapshot_index, event = response['match_snapshot'][0]
                ack_index, ack = response['command_done'][0]
                sampled_ns = event.get('sampled_monotonic_ns')
                snapshot_ns, ack_ns = event.get('monotonic_ns'), ack.get('monotonic_ns')
                if (all(type(value) is int for value in (sampled_ns, snapshot_ns, ack_ns))
                        and max(reader_created_ns, requests[role][1]) <= sampled_ns <= snapshot_ns <= ack_ns
                        and snapshot_index < ack_index):
                    found[role] = event
            return found

        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise TimeoutError('fresh_match_snapshot_deadline')
        pool.wait(lambda: len(snapshots()) == 2, remaining, 'fresh_match_snapshot_response')
        proof = snapshots()
        if all(type(e.get('matched_count')) is int and e['matched_count'] == 1 for e in proof.values()):
            return proof
        if time.monotonic() >= deadline:
            raise TimeoutError('fresh_match_snapshot_deadline')


def query_acknowledged_graphs(pool, events, timeout, emit=None, capture_graph=None):
    """Query once per role and wait for its unique snapshot -> graph -> ACK chain.

    Periodic or previous-command graph tokens never satisfy this query. These
    commands replace existing graph queries; they add no new DDS entities.
    """
    requests = {}
    for role in ('writer', 'reader'):
        requested_ns = time.monotonic_ns()
        command_id = pool.command(role, 'snapshot')
        requests[role] = (command_id, requested_ns)
        if emit is not None:
            emit('graph_snapshot_requested', role=role, command_id=command_id,
                 requested_monotonic_ns=requested_ns)

    def completed():
        proof = {}
        for role, (command_id, requested_ns) in requests.items():
            responses = {kind: [] for kind in ('match_snapshot', 'graph_snapshot', 'command_done')}
            for index, event in enumerate(events):
                kind = event.get('event')
                if (kind in responses and event.get('role') == role
                        and type(event.get('command_id')) is int and event['command_id'] == command_id):
                    responses[kind].append((index, event))
            if any(len(items) != 1 for items in responses.values()):
                continue
            si, snapshot = responses['match_snapshot'][0]
            gi, graph = responses['graph_snapshot'][0]
            ai, ack = responses['command_done'][0]
            stamps = [snapshot.get('sampled_monotonic_ns'), snapshot.get('monotonic_ns'),
                      graph.get('monotonic_ns'), ack.get('monotonic_ns')]
            if (all(type(stamp) is int for stamp in stamps)
                    and requested_ns <= stamps[0] <= stamps[1] <= stamps[2] <= stamps[3]
                    and si < gi < ai):
                proof[role] = {'snapshot': snapshot, 'graph': graph, 'ack': ack}
        return proof

    pool.wait(lambda: len(completed()) == 2, timeout, 'acknowledged_graph_query')
    proof = completed()
    if capture_graph is not None:
        for role in ('writer', 'reader'):
            capture_graph(proof[role]['graph'], proof[role]['ack'])
    return proof


def acquire_pair_environment(config, output_dir, emit, *, result=None):
    """Canonical three-lease + process/RTPS/source/environment preflight.

    Caller MUST keep the returned LeaseSet until every child has exited.
    Failures release here because no worker may exist before this returns.
    """
    lock_root = Path(config.get('lock_root', f'/tmp/r2f-runtime-locks-{os.getuid()}'))
    lease = LeaseSet(lock_root, output_dir, config['campaign_id'], config['domain_id'])
    lease.__enter__()
    try:
        emit('isolation_locks_acquired', locks=lease.receipts)
        environment = check_environment(config)
        if result is not None:
            result['environment'] = environment
        (Path(output_dir) / 'environment.json').write_text(
            json.dumps(environment, sort_keys=True, indent=2) + '\n')
        emit('environment_preflight', ok=environment['ok'], reason=environment.get('reason'),
             environment=environment)
        if not environment['ok']:
            raise IsolationError(environment.get('reason', 'environment_preflight_failed'))
        require_rmws_available(environment, config)
        _, child_environment = activated_environment(config)
        return lease, environment, child_environment
    except BaseException:
        lease.__exit__(None, None, None)
        raise


def verify_pair_lease(lease, config, output_dir):
    """Check actual open lease inodes/owners; config receipt strings cannot pass."""
    if not isinstance(lease, LeaseSet) or len(lease.handles) != 3 or len(lease.receipts) != 3:
        raise IsolationError('topology_requires_held_canonical_three_leases')
    if (lease.state_dir != Path(output_dir).resolve()
            or lease.campaign_id != config['campaign_id']
            or lease.domain_id != config['domain_id']):
        raise IsolationError('topology_lease_identity_mismatch')
    for handle, receipt in zip(lease.handles, lease.receipts):
        if handle.closed:
            raise IsolationError('topology_lease_inode_not_held')
        opened, named = os.fstat(handle.fileno()), Path(receipt['path']).stat()
        if (opened.st_dev, opened.st_ino) != (named.st_dev, named.st_ino):
            raise IsolationError('topology_lease_inode_not_held')
        # A nonempty receipt/open FD is not a held flock. An independently
        # opened description MUST conflict while this process holds the lease.
        import fcntl
        with Path(receipt['path']).open('r') as probe:
            try:
                fcntl.flock(probe, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                pass
            else:
                fcntl.flock(probe, fcntl.LOCK_UN)
                raise IsolationError('topology_lease_flock_not_held')
        handle.seek(0)
        owner = json.load(handle)
        if (owner.get('pid') != os.getpid() or owner.get('campaign_id') != config['campaign_id']
                or owner.get('domain_id') != config['domain_id']
                or owner.get('state_dir') != str(Path(output_dir).resolve())):
            raise IsolationError('topology_lease_owner_mismatch')
    return lease.receipts


def _observe_g01_pair(c, result, pool, emit, step, publish, observe):
    """Collect G01 inside run_pair's existing lease/source/cleanup lifetime."""
    events, receipt = result['events'], result['mutation_receipt']
    active = c['arm'] == 'mutated'
    def environment_ready():
        return (any(e['event'] == 'environment_preflight' and e.get('ok') is True for e in events)
                and any(e['event'] == 'isolation_locks_acquired' and e.get('locks') for e in events))
    step('admit_environment', environment_ready, lambda: None, c['startup_timeout_sec'],
         ['environment_ok', 'locks_held'])
    env_step = result['precondition_receipts'][-1]
    env_refs = [{'step_id': env_step['step_id'],
                 'raw_before_index': next(e['event_index'] for e in events
                     if e['event'] == 'state_step_before' and e['step_id'] == env_step['step_id']),
                 'raw_after_index': next(e['event_index'] for e in events
                     if e['event'] == 'state_step_after' and e['step_id'] == env_step['step_id'])}]
    receipt.update(submitted=active, carrier_admitted=active, configured=active)
    emit('mutation_begin' if active else 'control_begin',
         mutation_kind=c['mutation_kind'], mutation_receipt=dict(receipt),
         preconditions_snapshot={'environment_ok': result['environment'].get('ok') is True,
                                 'locks_held': True, 'endpoints_created': False},
         precondition_receipt_refs=env_refs)
    emit('window_open', window='pair_compatibility_window')
    tokens = {}
    for role in ('writer', 'reader'):
        def created(role=role):
            return any(e['event'] == 'endpoint_created' and e.get('role') == role for e in events)
        step(role + '_created', created, lambda role=role: pool.start(role),
             c['startup_timeout_sec'], ['environment_ok', 'locks_held'], observation=True)
        token = next(e for e in events if e['event'] == 'endpoint_created' and e.get('role') == role)
        require_role_rmw(c, role, token)
        if not token.get('endpoint_gid') or not token.get('endpoint_generation'):
            raise RuntimeError('g01_endpoint_identity_missing:' + role)
        tokens[role] = token
        observe('capture_worker', role, token['pid'], token)
        binding = wait_for_qos_binding(events, pool, role, token['endpoint_gid'],
                                       c['startup_timeout_sec'], emit=emit, exactly_one=True)
        from .relation_observer import g01_binding_readback
        g01_binding_readback(binding, token, c[role + '_qos'])
    if tokens['writer']['pid'] == tokens['reader']['pid']:
        raise RuntimeError('g01_endpoint_processes_not_independent')
    result['effective_rmws'] = effective_rmws_receipt(c, tokens)
    emit('qos_binding_verified', observation_api=(
         'per_role_public_endpoint_actual_qos_and_graph_readback'
         if all('actual_endpoint_qos' in next(e for e in events
             if e['event'] == 'qos_binding_observed' and e.get('role') == role)
             for role in tokens) else 'per_role_public_graph_readback'),
         endpoints={role: token['endpoint_gid'] for role, token in tokens.items()})
    from .relation_observer import g01_configuration_readback
    g01_configuration_readback(events, c)
    receipt.update(applied=active, hit=active)
    emit('configuration_intervention_observed',
         hit_kind='public_api_configuration_intervention',
         role_readback_refs={role: next(e['event_index'] for e in events
             if e['event'] == 'qos_binding_observed' and e.get('role') == role) for role in tokens},
         mutation_receipt=dict(receipt), relation_effect_observed=False)
    # A finite wait is observation, not a promise that even RR must match.
    # Query both roles even for BR; a prohibited match must not be suppressed
    # simply because the submitted policy says it should not exist.
    deadline = time.monotonic() + c['match_timeout_sec']
    while True:
        proof = query_acknowledged_graphs(pool, events, c['startup_timeout_sec'], emit)
        exact = all(
            {e.get('endpoint_gid') for e in proof[r]['graph']['publishers']} == {tokens['writer']['endpoint_gid']}
            and {e.get('endpoint_gid') for e in proof[r]['graph']['subscriptions']} == {tokens['reader']['endpoint_gid']}
            and proof[r]['snapshot'].get('endpoint_gid') == tokens[r]['endpoint_gid']
            and proof[r]['snapshot'].get('matched_count') == 1 for r in tokens)
        if exact or time.monotonic() >= deadline:
            break
        until = min(deadline, time.monotonic() + 0.05)
        pool.wait(lambda: time.monotonic() >= until, 1, 'g01_observation_interval')
    emit('pair_probe_begin', matched_snapshot_observed=exact,
         reliable_delivery_obligation=False,
         note='compatibility guard only; no BE reliable-delivery obligation')
    for sequence in range(c['post_match_count']):
        publish(sequence, 'pair_probe')
        until = time.monotonic() + c['publish_interval_sec']
        pool.wait(lambda: time.monotonic() >= until, c['publish_interval_sec'] + 1, 'pair_probe_interval')
    def received_all():
        return set(range(c['post_match_count'])) <= {
            e.get('send_sequence') for e in events if e['event'] == 'receive' and e.get('phase') == 'pair_probe'}
    try:
        pool.wait(received_all, c['delivery_timeout_sec'], 'g01_probe_observation_deadline')
        probe_timed_out = False
    except TimeoutError:
        probe_timed_out = True
    query_acknowledged_graphs(pool, events, c['startup_timeout_sec'], emit)
    emit('window_close', window='pair_compatibility_window', deadline_expired=probe_timed_out,
         timeout_is_sut_failure=False)
    from .relation_observer import g01_runtime_observation
    relation = g01_runtime_observation(events, c)
    result['relation_observation'] = relation
    result['relation_guard_facts'] = relation['facts']
    effect = active and relation['relation_effect_observed']
    receipt.update(effect_observed=effect)
    emit('mutation_effect' if effect else 'relation_observation_complete',
         effect_id='public_api_pair_match' if effect else None,
         hit_kind='public_api_configuration_intervention',
         mutation_receipt=dict(receipt), oracle_input=relation['oracle_input'],
         reasons=relation['reasons'], absence_is_not_proof=True)
    if not relation['oracle_input']:
        emit('discovery_observation_unknown',
             classification='relation_evidence_incomplete_not_sut_violation',
             reasons=relation['reasons'])


def run_pair(config: dict, output_dir: Path, *, topology_backend=None) -> dict:
    # Explicit opt-in extension, deliberately outside EXPLORATORY_TARGETS.
    from .topology_route import topology_route_requested, run_topology_route
    if topology_route_requested(config) or topology_backend is not None:
        return run_topology_route(config, output_dir, backend=topology_backend)
    output_dir = Path(output_dir).resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    result = {'real_execution': False, 'runtime_status': 'blocked', 'status': 'blocked', 'reason': None,
              'events': [], 'facts': {}, 'environment': {}, 'precondition_receipts': [], 'observation_receipts': [],
              'mutation_receipt': dict.fromkeys(RECEIPT_KEYS, False), 'execution': None,
              'execution_metadata': None, 'raw_path': None, 'raw_sha256': None,
              'capabilities': runtime_capabilities(), 'evidence_purpose': config.get('evidence_purpose', 'runtime_execution')}
    try:
        c = normalize_config(config)
    except (ValueError, TypeError) as exc:
        result['reason'] = 'blocked:invalid_config:' + str(exc)
        return result
    if c['target_id'] == 'discovery.interruption_recovery':
        if c['carrier'] != 'wire_proxy' or c['mutation_kind'] != 'discovery_drop':
            result['reason'] = 'blocked:discovery_drop_requires_wire_proxy_contract'
            return result
        from .namespace_pair import run_discovery_pair
        return run_discovery_pair(c, output_dir, result)
    if c['target_id'] == 'builtin.discovery_projection_reconciliation':
        if c['carrier'] != 'ros2_endpoint' or c['mutation_kind'] != 'endpoint_qos_variation':
            result['reason'] = 'blocked:projection_requires_public_endpoint_qos_contract'
            return result
        from .namespace_pair import run_discovery_pair
        return run_discovery_pair(c, output_dir, result)
    if c['target_id'] in EXPLORATORY_TARGETS:
        spec = EXPLORATORY_TARGETS[c['target_id']]
        if c['carrier'] != spec['carrier'] or c['mutation_kind'] != spec['mutation_kind']:
            result['reason'] = ('blocked:exploratory_carrier_contract_mismatch:'
                                + spec['carrier'] + '/' + spec['mutation_kind'])
            return result
        expected = []
        if c['arm'] == 'mutated' or c.get('fuzz_surfaces'):
            try:
                expected = exploratory_bindings(c)
                require_binding_preserved(expected, c)
            except (ValueError, TypeError) as exc:
                result['reason'] = 'blocked:exploratory_binding_error:' + str(exc)
                result['exploratory_bindings'] = []
                return result
        result['exploratory_bindings'] = expected
        result['runtime_capability'] = spec['runtime_capability']
        admission_error = recovery_fuzz_admission_error(c)
        if admission_error:
            result['reason'] = admission_error
            result['capability_status'] = 'unsupported_capability'
            return result
        if spec['backend'] == 'projection_pair':
            from .namespace_pair import run_discovery_pair
            # Dispatch only; the target id stays the exploratory id.
            return run_discovery_pair(c, output_dir, result)
        if spec['backend'] == 'discovery_namespace':
            from .namespace_pair import run_discovery_pair
            return run_discovery_pair(c, output_dir, result)
        # endpoint_pair fuzz targets use the existing public-API pair below.
    elif c['target_id'] not in ('discovery.late_endpoint', 'discovery.late_join_delivery'):
        result['reason'] = 'blocked:target_not_supported'
        return result
    relation_pair = c['target_id'] == 'discovery.match_compatibility.fuzz_v1'
    if relation_pair and (c['arm'] == 'negative' or c.get('fault_id')):
        result['reason'] = 'blocked:g01_negative_control_contract_not_implemented'
        return result
    if c['carrier'] != 'ros2_endpoint' or (not relation_pair and c['mutation_kind'] != 'late_joiner'):
        result['reason'] = 'blocked:endpoint_carrier_not_expressible'
        return result
    if c['target_id'] in EXPLORATORY_TARGETS and c.get('runtime_windows'):
        result['reason'] = 'blocked:endpoint_pair_does_not_apply_discovery_windows'
        return result
    if observation_uses_single_vendor_contract(c) and c.get('observation_mode', 'blackbox') == 'instrumentation':
        # Explicit vendor tracing is Cyclone-only and its capture identity
        # assumes one RMW for every role. Refuse rather than present half-pair
        # evidence; this also covers rmws that only overrides reader2/reader3.
        result['reason'] = 'blocked:mixed_vendor_instrumentation_not_supported'
        return result
    # Refuse to overwrite any evidence from an earlier invocation.
    raw_path = output_dir / 'raw.jsonl'
    try:
        raw = raw_path.open('x', buffering=1)
    except FileExistsError:
        result['reason'] = 'blocked:state_directory_already_executed'
        return result
    result['raw_path'] = str(raw_path)
    # The receipt is a fact about configuration until workers report; observed
    # identifiers stay null, never filled from the requested value.
    result['effective_rmws'] = effective_rmws_receipt(c)
    exploratory = c['target_id'] in EXPLORATORY_TARGETS
    receipts = result['precondition_receipts']
    observation_receipts = result['observation_receipts']
    receipt = result['mutation_receipt']
    active_mutation = c['arm'] == 'mutated'
    receipt['planned'] = active_mutation
    events = result['events']
    pool = None
    lease = None
    from .namespace_pair import execution_chain_identity, same_source
    from .instrumentation_bridge import prepare
    observation = None
    observation_errors = []
    before_source = execution_chain_identity()
    started_ns = time.monotonic_ns()

    def emit(event, **fields):
        value = {'event': event, 'time_ns': time.time_ns(), 'monotonic_ns': time.monotonic_ns(),
                 'role': 'controller', 'pid': os.getpid(), 'pgid': os.getpgrp(),
                 'run_id': c['run_id'], 'domain_id': c['domain_id'], 'rmw_identifier': None}
        value.update(fields)
        value['event_index'] = len(events)
        events.append(value)
        raw.write(json.dumps(value, sort_keys=True) + '\n')
        if event == 'endpoint_created':
            result['real_execution'] = True

    # Record the configured (pre-observation) receipt in raw before any
    # preflight can block, so a blocked mixed run is still auditable.
    emit('effective_rmws', receipt=result['effective_rmws'],
         mixed=result['effective_rmws']['mixed'])

    def observe(method, *args):
        # Observation errors are sidecar diagnostics, not primary SUT failures.
        try:
            response = getattr(observation, method)(*args)
            if isinstance(response, dict) and response.get('accepted') is False:
                observation_errors.append({'hook': method, 'reason': response.get('reason', 'rejected')})
            return response
        except (OSError, ValueError, TypeError, RuntimeError) as exc:
            observation_errors.append({'hook': method, 'reason': type(exc).__name__})
            return None

    def capture_graph(graph, ack):
        return observe('capture_graph', graph, ack)

    if exploratory:
        emit('exploratory_binding_submitted',
             bindings=result.get('exploratory_bindings', []),
             submitted_parameters=c.get('submitted_parameters', {}),
             runtime_capability=result.get('runtime_capability'))

    def has(event, role=None, **fields):
        return any(e['event'] == event and (role is None or e.get('role') == role) and all(e.get(k) == v for k, v in fields.items()) for e in events)

    def step(name, predicate, action, timeout, requirement, *, observation=False):
        record = {'step_id': name, 'requires': requirement, 'before': {'monotonic_ns': time.monotonic_ns(), 'satisfied': bool(predicate())}, 'after': None, 'satisfied': False}
        record['receipt_kind'] = 'observation' if observation else 'precondition'
        (observation_receipts if observation else receipts).append(record)
        emit('state_step_before', **record)
        try:
            action()
            pool.wait(predicate, timeout, name)
            record['satisfied'] = True
        finally:
            record['after'] = {'monotonic_ns': time.monotonic_ns(), 'satisfied': bool(predicate())}
            emit('state_step_after', **record)

    def publish(sequence, phase, omit=False):
        command_id = pool.command('writer', 'publish', send_sequence=sequence, phase=phase, omit=omit)
        pool.wait(lambda: has('command_done', 'writer', command_id=command_id), c['startup_timeout_sec'], 'publish_ack')

    try:
        lease, environment, child_environment = acquire_pair_environment(c, output_dir, emit, result=result)
        result['environment'] = environment
        # prepare must succeed before worker creation: never fall back to an
        # unsanitized environment after a blackbox preparation failure.
        if observation_uses_single_vendor_contract(c):
            # The passive observation bridge currently has a single-vendor
            # profile contract. Mixed vendor runs still execute through the
            # public API, but the sidecar is explicitly unavailable rather than
            # pretending one vendor's SEDP trace observed both.
            observation = _MixedObservationContext(c, child_environment)
            role_env = role_rmw_environments(c, observation.role_env)
            # Each role keeps only its own vendor's profile variable; a leftover
            # variable from the other vendor would redirect transport selection.
            for role, env in role_env.items():
                rmw = role_rmw(c, role)
                if rmw != 'rmw_cyclonedds_cpp':
                    env.pop('CYCLONEDDS_URI', None)
                if rmw != 'rmw_fastrtps_cpp':
                    env.pop('FASTRTPS_DEFAULT_PROFILES_FILE', None)
                    env.pop('FASTDDS_DEFAULT_PROFILES_FILE', None)
            emit('mixed_vendor_observation_unsupported',
                 reason='single_vendor_passive_sidecar_contract')
            pool = RoleRmwWorkerPool(c, observation.base_env, output_dir, emit,
                                     role_environments=role_env)
        else:
            observation = prepare(c, output_dir, child_environment)
            role_env = role_rmw_environments(c, observation.role_env)
            pool_class = RoleRmwWorkerPool if role_rmw_differs_from_config(c) else WorkerPool
            pool = pool_class(c, observation.base_env, output_dir, emit,
                              role_environments=role_env)
        if relation_pair:
            _observe_g01_pair(c, result, pool, emit, step, publish, observe)
        else:
            emit('window_open', window='before_reader_match')
            step('writer_created', lambda: has('endpoint_created', 'writer'), lambda: pool.start('writer'),
                 c['startup_timeout_sec'], ['environment_ok', 'locks_held'])
            writer_token = next(e for e in events if e['event'] == 'endpoint_created' and e['role'] == 'writer')
            require_role_rmw(c, 'writer', writer_token)
            if not writer_token.get('endpoint_gid'):
                raise RuntimeError('writer_identity_or_rmw_incomplete')
            observed_rmws = {'writer': writer_token}
            result['effective_rmws'] = effective_rmws_receipt(c, observed_rmws)
            emit('effective_rmws_observed', role='writer',
                 expected=role_rmw(c, 'writer'),
                 observed=writer_token.get('rmw_identifier'),
                 receipt=result['effective_rmws'])
            observe('capture_worker', 'writer', writer_token['pid'], writer_token)
            step('writer_prejoin_published', lambda: sum(e['event'] == 'send' and e.get('phase') == 'prejoin' for e in events) == c['pre_match_count'],
                 lambda: [publish(i, 'prejoin') for i in range(c['pre_match_count'])], c['startup_timeout_sec'], ['writer_created'])
            preconditions_ok = len(receipts) == 2 and all(r['satisfied'] for r in receipts)
            if not preconditions_ok:
                raise RuntimeError('mutation_preconditions_not_reached')
            references = [{'step_id': r['step_id'],
                           'raw_before_index': next(e['event_index'] for e in events if e['event'] == 'state_step_before' and e['step_id'] == r['step_id']),
                           'raw_after_index': next(e['event_index'] for e in events if e['event'] == 'state_step_after' and e['step_id'] == r['step_id'])} for r in receipts]
            snapshot = {'writer_created': True, 'writer_prejoin_published': True, 'reader_created': False,
                        'writer_gid': writer_token['endpoint_gid'],
                        'prejoin_publish_sequences': [e['send_sequence'] for e in events if e['event'] == 'send' and e.get('phase') == 'prejoin']}
            receipt['submitted'] = active_mutation
            receipt['carrier_admitted'] = active_mutation
            receipt['configured'] = active_mutation
            emit('mutation_begin' if active_mutation else 'control_begin',
                 mutation_kind=c['mutation_kind'], preconditions_snapshot=snapshot,
                 precondition_receipt_refs=references, mutation_receipt=dict(receipt))
            # transparent and baseline execute the same input, including the delay.
            delay = c['reader_delay_sec'] if active_mutation else c['baseline_reader_delay_sec']
            delay_deadline = time.monotonic() + delay
            pool.wait(lambda: time.monotonic() >= delay_deadline, delay + 1, 'reader_creation_delay')
            step('reader_created', lambda: has('endpoint_created', 'reader'), lambda: pool.start('reader'),
                 c['startup_timeout_sec'], ['writer_created', 'writer_prejoin_published'], observation=True)
            reader_token = next(e for e in events if e['event'] == 'endpoint_created' and e['role'] == 'reader')
            require_role_rmw(c, 'reader', reader_token)
            if not reader_token.get('endpoint_gid') or reader_token['pid'] == writer_token['pid']:
                raise RuntimeError('reader_identity_or_process_isolation_incomplete')
            observed_rmws['reader'] = reader_token
            result['effective_rmws'] = effective_rmws_receipt(c, observed_rmws)
            emit('effective_rmws_observed', role='reader',
                 expected=role_rmw(c, 'reader'),
                 observed=reader_token.get('rmw_identifier'),
                 receipt=result['effective_rmws'])
            observe('capture_worker', 'reader', reader_token['pid'], reader_token)
            actual_delay = reader_token['created_monotonic_ns'] - writer_token['created_monotonic_ns']
            if actual_delay <= 0:
                raise RuntimeError('reader_not_created_after_writer')
            emit('reader_created_after_writer', writer_gid=writer_token['endpoint_gid'], reader_gid=reader_token['endpoint_gid'],
                 delay_ns=actual_delay, configured_delay_sec=delay)
            if exploratory:
                requested_by_role = {role: c[role + '_qos'] for role in ('writer', 'reader')}
                submitted_qos = c.get('submitted_parameters', {})
                for role, token in (('writer', writer_token), ('reader', reader_token)):
                    if token.get('qos') != requested_by_role[role]:
                        raise RuntimeError('endpoint_reported_qos_differs_from_config:' + role)
                    # The worker emits qos_binding_observed on its own creation path;
                    # that line can still be unread in the pipe when pool.start()
                    # returns.  Wait for it (bounded) instead of reading a stale
                    # event list.  GID binding and non-empty observation stay strict.
                    wait_for_qos_binding(events, pool, role, token['endpoint_gid'],
                                         c['startup_timeout_sec'], emit=emit,
                                         exactly_one=False)
                observed = {
                    'P01': {'configured_delay_sec': delay, 'observed_delay_ns': actual_delay,
                            'basis': 'reader_created_monotonic_minus_writer_created_monotonic'},
                    'W01': {'depth': c['qos']['depth'],
                            'pre_match_count': c['pre_match_count'],
                            'post_match_count': c['post_match_count'],
                            'basis': 'config_and_actual_publish_counts'},
                    'W02': {'publish_interval_sec': c['publish_interval_sec'],
                            'basis': 'actual_inter_publish_monotonic_delta'},
                }
                selected = {sid: observed[sid] for sid in c.get('fuzz_surfaces', []) if sid in observed}
                binding_event_index = len(events)
                emit('exploratory_binding_observed', observed=selected,
                     requested_qos=requested_by_role,
                     submitted_qos=submitted_qos,
                     observation_basis='public_api_endpoint_and_creation_events')
                emit('qos_binding_verified', endpoints=requested_by_role,
                     observation_api='get_publishers_info_by_topic/get_subscriptions_info_by_topic',
                     binding_event_ref=binding_event_index)
            if active_mutation:
                # A submitted mutation is not automatically a hit.  P01's zero
                # delay is a legitimate boundary input but has no observable timing
                # effect; record it as no_effect instead of fabricating hit=True.
                timing_hit = (c.get('mutation_kind') != 'late_joiner'
                              or bool(c.get('fuzz_surfaces'))
                              and (delay > 0.0 and actual_delay > 0))
                receipt.update(applied=True, hit=timing_hit, effect_observed=timing_hit)
                emit('mutation_effect' if timing_hit else 'mutation_no_effect',
                     effect_id='reader_created_after_writer', mutation_receipt=dict(receipt),
                     configured_delay_sec=delay, observed_delay_ns=actual_delay,
                     reason=None if timing_hit else 'zero_delay_has_no_observable_timing_effect')
            elif c['arm'] == 'transparent':
                emit('transparency_control', declaration={'mutation_kind': c['mutation_kind'], 'parameters': c['mutation_parameters']},
                     applied_input_equal_to_baseline=True, effective_reader_delay_sec=delay, mutation_hit=False)
            fresh_matches = {}
            def observe_fresh_matches():
                fresh_matches.update(wait_for_fresh_match(
                    pool, events, reader_token['created_monotonic_ns'], c['match_timeout_sec'], emit))
            step('endpoints_matched', lambda: set(fresh_matches) == {'writer', 'reader'},
                 observe_fresh_matches, c['match_timeout_sec'], ['reader_created', 'writer_created'], observation=True)
            # Check only the newly requested, fully acknowledged graph projections.
            graph_proof = {}
            def graph_expected():
                for role in ('writer', 'reader'):
                    if role not in graph_proof:
                        return False
                    snapshot = graph_proof[role]['graph']
                    if {p['endpoint_gid'] for p in snapshot['publishers']} != {writer_token['endpoint_gid']}:
                        return False
                    if {p['endpoint_gid'] for p in snapshot['subscriptions']} != {reader_token['endpoint_gid']}:
                        return False
                return True
            def observe_graph():
                graph_proof.update(query_acknowledged_graphs(pool, events, c['match_timeout_sec'], emit, capture_graph))
            step('graph_identity_verified', graph_expected, observe_graph, c['match_timeout_sec'], ['endpoints_matched'], observation=True)
            emit('window_close', window='before_reader_match')
            expected = list(range(c['pre_match_count'], c['pre_match_count'] + c['post_match_count']))
            omitted = expected[len(expected) // 2] if c['fault_id'] else None
            emit('delivery_contract', expected_sequences=expected, sentinel_sequence=expected[-1],
                 fault_id=c['fault_id'], contract_includes_harness_omission=True,
                 prejoin_in_delivery_contract=False, qos=c['qos'])
            emit('window_open', window='post_match_delivery', expected_sequences=expected)
            for sequence in expected:
                publish(sequence, 'post_match', omit=sequence == omitted)
                pause_deadline = time.monotonic() + c['publish_interval_sec']
                pool.wait(lambda: time.monotonic() >= pause_deadline, c['publish_interval_sec'] + 1, 'publish_interval')
            # A fault omission is intentional absence before publish, not SUT data loss.
            def delivered():
                received = {e['send_sequence'] for e in events if e['event'] == 'receive' and e.get('phase') == 'post_match'}
                return set(expected) - ({omitted} if omitted is not None else set()) <= received
            try:
                pool.wait(delivered, c['delivery_timeout_sec'], 'delivery_deadline')
                delivery_timed_out = False
            except TimeoutError:
                delivery_timed_out = True
            if exploratory:
                sends = [e for e in events if e['event'] == 'send' and e.get('phase') == 'post_match']
                deltas = [b['monotonic_ns'] - a['monotonic_ns'] for a, b in zip(sends, sends[1:])]
                actual_interval = (sum(deltas) / len(deltas) / 10**9) if deltas else None
                tolerance = max(0.002, c['publish_interval_sec'] * 0.5)
                # W02 is a requested pacing lower bound, not a throughput claim.
                # The controller waits for publish ACK before the next interval, so
                # the observed inter-send gap is >= configured; a gap below the
                # configured lower bound is the actual binding violation.
                interval_bound = (actual_interval is not None
                                  and actual_interval >= c['publish_interval_sec'] - tolerance)
                if 'W02' in c.get('fuzz_surfaces', []) and not interval_bound:
                    raise RuntimeError('publish_interval_binding_not_observed')
                emit('timing_binding_observed', configured_publish_interval_sec=c['publish_interval_sec'],
                     observed_mean_publish_interval_sec=actual_interval,
                     samples=len(sends), tolerance_sec=tolerance,
                     send_schedule_refs=[e['event_index'] for e in sends])
            emit('window_close', window='post_match_delivery', deadline_expired=delivery_timed_out,
                 intended_missing_includes_omission=True)
            query_acknowledged_graphs(pool, events, c['startup_timeout_sec'], emit, capture_graph)
            identity_facts = summarize_events(events)
            emit('identity_observation', identity_basis=identity_facts['identity_basis'], graph_boundary_check=True,
                 publisher_gid_status=identity_facts['publisher_gid_status'],
                 graph_stable_across_window=identity_facts['graph_stable_across_window'],
                 endpoint_identity_correlated=identity_facts['endpoint_identity_correlated'],
                 limitations=identity_facts['identity_limitations'])
            if c['fault_id']:
                emit('fault_control_receipt', fault_id=c['fault_id'], omitted_sequence=omitted,
                     applied=has('harness_omission', 'writer', send_sequence=omitted),
                     responsibility_layer='harness', candidate_eligible=False)
        after_source = execution_chain_identity()
        if not same_source(before_source, after_source):
            raise RuntimeError('source_identity_changed_during_execution')
        result.update(runtime_status='completed', status='completed', reason=None)
    except (IsolationError, OSError, RuntimeError, TimeoutError, ValueError) as exc:
        reason = str(exc) if result['real_execution'] else mixed_worker_start_failure(c, str(exc))
        if reason != str(exc):
            emit('mixed_vendor_worker_failure_blocked', original_reason=str(exc), reason=reason)
        result.update(runtime_status='failed' if result['real_execution'] else 'blocked',
                      status='failed' if result['real_execution'] else 'blocked', reason=reason)
        emit('runtime_abort', reason=reason)
    finally:
        if pool is not None:
            try:
                pool.close()
            except (OSError, RuntimeError, TimeoutError) as exc:
                result.update(runtime_status='failed', status='failed', reason='worker_cleanup_error:' + str(exc))
                emit('runtime_abort', reason=result['reason'])
            # A failed shutdown invalidates process health even if messages arrived.
            if any(m.get('returncode') != 0 for m in pool.metadata) and result['runtime_status'] == 'completed':
                result.update(runtime_status='failed', status='failed', reason='worker_shutdown_failed')
        final_source = execution_chain_identity()
        if not same_source(before_source, final_source):
            result.update(runtime_status='failed' if result['real_execution'] else 'blocked',
                          status='failed' if result['real_execution'] else 'blocked',
                          reason='source_identity_changed_during_execution')
            emit('runtime_abort', reason=result['reason'])
        if relation_pair:
            from .relation_observer import g01_runtime_observation
            relation = g01_runtime_observation(events, c)
            result['relation_observation'] = relation
            result['relation_guard_facts'] = relation['facts']
            result['facts'] = relation['facts'] or {}
        else:
            result['facts'] = summarize_events(events)
        observed = observed_rmws_from_events(events)
        if observed:
            result['effective_rmws'] = effective_rmws_receipt(c, observed)
        if result['real_execution']:
            result['execution'] = {'real_execution': True, 'run_id': c['run_id'], 'target_id': c['target_id'],
                                   'target_revision': c['target_revision'], 'rmw': c['rmw'], 'domain_id': c['domain_id'],
                                   'effective_rmws': result['effective_rmws'],
                                   'arm': c['arm'], 'repeat': c['repeat'], 'runtime_status': result['runtime_status'],
                                   'started_monotonic_ns': started_ns, 'finished_monotonic_ns': time.monotonic_ns(),
                                   'workers': pool.metadata if pool else [], 'source': before_source, 'source_after': final_source,
                                   'precondition_receipts': receipts, 'observation_receipts': observation_receipts, 'mutation_receipt': dict(receipt),
                                   'fault_control': bool(c['fault_id']), 'candidate_eligible': not bool(c['fault_id'])}
            result['execution_metadata'] = result['execution']
        emit('runtime_end', runtime_status=result['runtime_status'], real_execution=result['real_execution'],
             mutation_receipt=dict(receipt), reason=result['reason'])
        raw.flush()
        os.fsync(raw.fileno())
        raw.close()
        result['raw_sha256'] = hashlib.sha256(raw_path.read_bytes()).hexdigest()
        try:
            if result['execution'] is not None:
                result['execution'].update(raw_path=str(raw_path), raw_sha256=result['raw_sha256'])
            if observation is not None:
                sidecar = observe('collect', result)
                if not isinstance(sidecar, dict):
                    sidecar = {'schema': 'r2f.runtime-instrumentation/v1',
                               'observation_mode': c.get('observation_mode', 'blackbox'),
                               'status': 'incomplete', 'verdict': 'unknown',
                               'intake': [], 'candidate_count': 0}
                if observation_errors:
                    sidecar.update(status='incomplete', verdict='unknown', intake=[], candidate_count=0,
                                   hook_diagnostics=list(observation_errors))
                result['observation'] = sidecar
                if result['execution'] is not None:
                    result['execution']['observation'] = sidecar
            if result['execution'] is not None:
                (output_dir / 'execution.json').write_text(json.dumps(result['execution'], sort_keys=True, indent=2) + '\n')
            (output_dir / 'runtime_result.json').write_text(json.dumps(result, sort_keys=True, indent=2) + '\n')
        finally:
            if lease is not None:
                lease.__exit__(None, None, None)
    return result
