"""Pure exploratory input projections, shared by planning and capsule replay.

Profile controls are submitted input, not execution metadata.  Resolve aliases
and carrier defaults *before* mutation/composition.  No ROS, process, source or
filesystem operations occur here.  Absent controls add no defaults (legacy v2
identities stay unchanged).
"""
from copy import deepcopy
import math

from r2f.core.evidence import canonical_hash


RECOVERY_FIELDS = frozenset({
    'reader_recreate_in_window', 'reader_recreate_delay_sec',
    'reader_recreate_at_fraction', 'reader_recreate_count',
    'reader_recreate_interval_sec', 'reader_recreate_fractions',
    'reader_destroy_fractions', 'writer_churn_in_window',
    'publish_burst_in_window', 'publish_burst_count',
    'publish_burst_interval_sec', 'environment_mutation', 'netem_covers_delivery',
})
G01_TARGET = "discovery.match_compatibility.fuzz_v1"


def is_g01_profile(profile):
    return profile.get("target_ids") == [G01_TARGET]


def validate_g01_profile(profile):
    """Exact-target activation, not a relaxation of the F2 windows opt-in."""
    if not is_g01_profile(profile):
        return False
    if profile.get("runtime_capability") is not None or profile.get("surfaces") != ["G01"]:
        raise ValueError("g01_requires_exact_target_and_surface_without_capability")
    # Explicit G01 profile copies may opt in; this is not live admission.
    # Other F2/default profiles retain their existing disabled-only contract.
    if type(profile.get("enabled")) is not bool or profile.get("required_catalog_included") is not False:
        raise ValueError("g01_requires_explicit_boolean_exploratory_profile")
    if type(profile.get("chain_length", 1)) is not int or profile.get("chain_length", 1) != 1:
        raise ValueError("g01_requires_chain_length_1")
    if set(profile.get("sampling_kinds", [])) - {"representative"}:
        raise ValueError("g01_only_pair_representatives_supported")
    if RECOVERY_FIELDS & set(profile) or PROJECTION_FIELDS & set(profile):
        raise ValueError("g01_additional_runtime_controls_not_enabled")
    if "search" in profile and profile["search"].get("samples_per_class") != 4:
        raise ValueError("g01_bfs_requires_four_explicit_pair_slots")
    return True


F2_WINDOWS_CAPABILITY = 'f2_windows_v1'
F2_WINDOWS_TARGET = 'discovery.interruption_recovery.fuzz_windows_v1'
F2_ANCHOR_POLICY = 'f2_anchor_not_before_v1'
F2_ANCHOR_TOLERANCE = 0.5
F2_SURFACE_MUTATORS = {'E02': 'M07', 'E03': 'M06'}
F2_TIMING_FIELDS = frozenset({'runtime_capability', 'anchor_timing_policy',
                              'anchor_lateness_tolerance_sec'})

PROJECTION_FIELDS = frozenset({'projection_roles', 'projection_role_qos'})
CONTROL_FIELDS = RECOVERY_FIELDS | PROJECTION_FIELDS | F2_TIMING_FIELDS | {'rmws'}
PROFILE_CONTROL_FIELDS = (CONTROL_FIELDS - {'rmws'}) | {'role_rmws'}
# These are declarations/provenance, never a blanket runtime passthrough.
PROFILE_METADATA_FIELDS = frozenset({
    'schema_version', 'profile_id', 'campaign_kind', 'enabled',
    'required_catalog_included', 'target_ids', 'surfaces', 'rmws',
    'seed_specs', 'seed_catalog_status', 'description', 'execution_boundary',
    'budget', 'corpus', 'controls', 'composition', 'cycles',
    'declared_seed_specs_union', 'pending_seed_ids', 'pending_surfaces',
    'sampling_kinds', 'sampling_weight', 'chain_length',
    'activation', 'cycle_id', 'runner_slice', 'mutator_whitelist',
    'search', 'scheduler_policy', 'topology_search',
})


def validate_search_profile(profile):
    """Search declarations are input metadata, never runtime/evidence knobs.

    Only declared-route F1 input BFS is currently implemented. Omitted search
    and policy fields preserve the legacy path and add no profile defaults.
    """
    policy = profile.get('scheduler_policy')
    if 'scheduler_policy' in profile and policy not in ('feedback', 'fair', 'bfs'):
        raise ValueError('unsupported profile scheduler_policy')
    if 'topology_search' in profile and 'search' not in profile:
        raise ValueError('topology_search requires explicit bounded search')
    if 'search' not in profile:
        if policy == 'bfs':
            raise ValueError('bfs profile requires explicit search settings')
        return
    if policy not in (None, 'bfs'):
        raise ValueError('input_search_bfs search requires bfs scheduler policy')
    from .search_frontier import search_settings
    from .mutation_routes import routes_enabled
    search_settings(profile['search'])
    if 'topology_search' in profile:
        from .topology_search import descriptor, validate_chain_length
        from r2f.modules.s1.topology_targets import TOPOLOGY_TARGET
        if profile.get('target_ids') != [TOPOLOGY_TARGET]:
            raise ValueError('topology_search requires exact topology target')
        desc = descriptor(profile['topology_search'], profile.get('operator_whitelist', []))
        validate_chain_length(profile, desc)
        if 'sampling_kinds' in profile or 'sampling_weight' in profile:
            raise ValueError('topology_search uses explicit classes, not F1 sampling kinds')
        return
    if not routes_enabled(profile):
        raise ValueError('input_search_bfs currently requires declared F1 mutation routes')
    if not isinstance(profile.get('sampling_kinds'), list) or not profile['sampling_kinds']:
        raise ValueError('input_search_bfs requires explicit sampling_kinds')


def validate_profile_fields(profile):
    """Reject misspellings/unsupported runtime knobs, rather than ignore them."""
    validate_search_profile(profile)
    from r2f.modules.s1.topology_targets import TOPOLOGY_CAPABILITY, TOPOLOGY_TARGET
    if profile.get('runtime_capability') == TOPOLOGY_CAPABILITY or (
            isinstance(profile.get('target_ids'), list) and TOPOLOGY_TARGET in profile['target_ids']):
        validate_topology_profile(profile)
        return
    unknown = set(profile) - PROFILE_METADATA_FIELDS - PROFILE_CONTROL_FIELDS
    if unknown:
        raise ValueError('unsupported exploratory profile field:' + ','.join(sorted(unknown)))
    validate_f2_profile(profile)
    validate_g01_profile(profile)
    for key in PROFILE_CONTROL_FIELDS & set(profile):
        if profile[key] is None:
            raise ValueError(key + ' must not be null; omit an unused control')



def validate_f2_profile(profile):
    """Explicit, target-scoped F2 opt-in; never promotes a catalog template.

    Absence is the unchanged F1 path. A new target without its capability is
    refused, as is any unknown capability or cross-surface/target combination.
    This validates declarations only, not observation or permission to run ROS.
    """
    capability = profile.get('runtime_capability')
    if capability is None:
        targets = profile.get('target_ids')
        if F2_TIMING_FIELDS & set(profile) or (
                isinstance(targets, (list, tuple)) and F2_WINDOWS_TARGET in targets):
            raise ValueError('f2_windows_runtime_capability_required')
        return False
    if capability != F2_WINDOWS_CAPABILITY:
        raise ValueError('unsupported runtime_capability:' + str(capability))
    if profile.get('target_ids') != [F2_WINDOWS_TARGET]:
        raise ValueError('f2_windows_requires_dedicated_target')
    if profile.get('enabled') is not False or profile.get('required_catalog_included') is not False:
        raise ValueError('f2_windows_requires_disabled_exploratory_profile')
    if type(profile.get('chain_length')) is not int or profile['chain_length'] != 1:
        raise ValueError('f2_windows_requires_chain_length_1')
    if 'sampling_kinds' in profile or 'sampling_weight' in profile:
        raise ValueError('f2_windows_wide_sampling_not_enabled')
    if profile.get('anchor_timing_policy') != F2_ANCHOR_POLICY or \
            type(profile.get('anchor_lateness_tolerance_sec')) not in (int, float) or \
            profile['anchor_lateness_tolerance_sec'] != F2_ANCHOR_TOLERANCE:
        raise ValueError('f2_windows_anchor_timing_contract_mismatch')
    surfaces = profile.get('surfaces')
    if not isinstance(surfaces, list) or not surfaces or \
            any(not isinstance(s, str) or s not in F2_SURFACE_MUTATORS for s in surfaces) or \
            len(set(surfaces)) != len(surfaces):
        raise ValueError('f2_windows_surface_not_enabled')
    if profile.get('mutator_whitelist') != {s: [F2_SURFACE_MUTATORS[s]] for s in surfaces}:
        raise ValueError('f2_windows_mutator_whitelist_mismatch')
    cycles = profile.get('cycles')
    if not isinstance(cycles, list) or not cycles:
        raise ValueError('f2_windows_requires_explicit_cycles')
    for cycle in cycles:
        if isinstance(cycle, dict) and set(cycle) - {
                'cycle_id', 'target_id', 'surfaces', 'seed_specs', 'runner_slice'}:
            raise ValueError('f2_windows_unsupported_cycle_field')
        if not isinstance(cycle, dict) or cycle.get('target_id') != F2_WINDOWS_TARGET or \
                not isinstance(cycle.get('surfaces'), list) or len(cycle['surfaces']) != 1 or \
                cycle['surfaces'][0] not in surfaces:
            raise ValueError('f2_windows_cycle_surface_or_target_mismatch')
        specs = cycle.get('seed_specs')
        if not isinstance(specs, list) or not specs or any(
                not isinstance(spec, dict) or spec.get('seed_id') != cycle['surfaces'][0]
                or spec.get('target_id') != F2_WINDOWS_TARGET for spec in specs):
            raise ValueError('f2_windows_cycle_seed_mismatch')
    # Netem/G01/W01 and new stimulus combinations are not this capability.
    if RECOVERY_FIELDS & set(profile) or PROJECTION_FIELDS & set(profile):
        raise ValueError('f2_windows_additional_runtime_controls_not_enabled')
    return True


def _number(value, field, low, high, *, upper_open=False):
    if type(value) not in (int, float) or not math.isfinite(value) \
            or value < low or (value >= high if upper_open else value > high):
        raise ValueError('invalid ' + field)


def _controls(config, backend):
    """Validate inherited as well as newly submitted controls, in place."""
    present = CONTROL_FIELDS & set(config)
    if present & F2_TIMING_FIELDS:
        if backend != 'discovery_namespace' or not F2_TIMING_FIELDS <= present:
            raise ValueError('f2_windows_incomplete_or_wrong_backend_controls')
        if (RECOVERY_FIELDS | PROJECTION_FIELDS) & set(config):
            raise ValueError('f2_windows_additional_runtime_controls_not_enabled')
        if any(k in config for k in ('anchor_observed', 'post_match_observed')):
            raise ValueError('f2_windows_observation_must_come_from_runtime')
        if (config['runtime_capability'] != F2_WINDOWS_CAPABILITY or
                config['anchor_timing_policy'] != F2_ANCHOR_POLICY or
                type(config['anchor_lateness_tolerance_sec']) not in (int, float) or
                config['anchor_lateness_tolerance_sec'] != F2_ANCHOR_TOLERANCE):
            raise ValueError('f2_windows_anchor_timing_contract_mismatch')
    for fields, expected in ((RECOVERY_FIELDS, 'discovery_namespace'),
                             (PROJECTION_FIELDS, 'projection_pair')):
        if present & fields and backend != expected:
            raise ValueError('unsupported controls for ' + backend + ':'
                             + ','.join(sorted(present & fields)))
    for key in present:
        if config[key] is None:
            raise ValueError(key + ' must not be null')
    for key in ('reader_recreate_in_window', 'writer_churn_in_window',
                'publish_burst_in_window', 'netem_covers_delivery'):
        if key in config and type(config[key]) is not bool:
            raise ValueError(key + ' must be a boolean')
    for key, maximum in (('reader_recreate_count', 8), ('publish_burst_count', 4096)):
        if key in config and (type(config[key]) is not int or not 1 <= config[key] <= maximum):
            raise ValueError('invalid ' + key)
    for key in ('reader_recreate_delay_sec', 'reader_recreate_interval_sec',
                'publish_burst_interval_sec'):
        if key in config:
            _number(config[key], key, 0, 120)
    if 'reader_recreate_at_fraction' in config:
        _number(config['reader_recreate_at_fraction'], 'reader_recreate_at_fraction',
                0, 1, upper_open=True)
    for key in ('reader_recreate_fractions', 'reader_destroy_fractions'):
        if key in config:
            values = config[key]
            if not isinstance(values, list) or not values:
                raise ValueError(key + ' must be a nonempty list')
            for value in values:
                _number(value, key, 0, 1, upper_open=True)
            if any(a >= b for a, b in zip(values, values[1:])):
                raise ValueError(key + ' must be strictly increasing')
    reader_settings = {key for key in present if key.startswith('reader_recreate_')
                       and key != 'reader_recreate_in_window'} | (present & {'reader_destroy_fractions'})
    if reader_settings and config.get('reader_recreate_in_window') is not True:
        raise ValueError('reader recreation settings require reader_recreate_in_window=true')
    if 'reader_recreate_fractions' in present and present & {
            'reader_recreate_at_fraction', 'reader_recreate_delay_sec', 'reader_recreate_interval_sec'}:
        raise ValueError('reader_recreate_fractions conflicts with delay/fraction/interval settings')
    if {'reader_recreate_at_fraction', 'reader_recreate_delay_sec'} <= present:
        raise ValueError('reader_recreate_at_fraction conflicts with reader_recreate_delay_sec')
    if present & {'publish_burst_count', 'publish_burst_interval_sec'} \
            and config.get('publish_burst_in_window') is not True:
        raise ValueError('publish burst settings require publish_burst_in_window=true')
    if 'environment_mutation' in present:
        from r2f.runtime.namespace_pair import normalize_environment_mutation
        config['environment_mutation'] = normalize_environment_mutation(config['environment_mutation'])
    environment = config.get('environment_mutation')
    if config.get('netem_covers_delivery') is True and (
            not isinstance(environment, dict) or environment.get('kind') != 'discovery_netem'):
        raise ValueError('netem_covers_delivery requires discovery_netem')
    if 'rmws' in present:
        from r2f.runtime.ros2_pair import _validate_rmws
        if not isinstance(config['rmws'], dict) or not config['rmws']:
            raise ValueError('rmws must be a nonempty role mapping')
        _validate_rmws(config['rmws'])
    if present & PROJECTION_FIELDS:
        from r2f.runtime.projection_pair import projection_roles, validate_qos
        roles = projection_roles(config)
        if 'projection_role_qos' in present:
            overrides = config['projection_role_qos']
            if not isinstance(overrides, dict) or not overrides:
                raise ValueError('projection_role_qos must be a nonempty role mapping')
            if set(overrides) - set(roles):
                raise ValueError('projection_role_qos contains an unknown role')
            for role, qos in overrides.items():
                if not isinstance(qos, dict) or not qos:
                    raise ValueError('projection_role_qos.' + role + ' must be a nonempty QoS mapping')
                validate_qos(qos)
    return config


def apply_profile_controls(parent, profile, *, backend):
    """Return the composition parent with validated profile controls applied.

    ``role_rmws`` is a profile-only alias; the submitted/capsule spelling is
    ``rmws``.  The profile's vendor selection list is never runtime input.
    """
    validate_profile_fields(profile)
    result = deepcopy(parent)
    for key in PROFILE_CONTROL_FIELDS & set(profile):
        result['rmws' if key == 'role_rmws' else key] = deepcopy(profile[key])
    return _controls(result, backend)


def runtime_input(final_input, *, backend, runtime_capability=None):
    """Rebuild the top-level runtime payload solely from capsule final_input.

    Execution metadata is added by the caller.  Role QoS is a deterministic
    projection of the final mutated QoS, followed by explicit role overrides
    in normalize_projection_config.  Neither profile nor historical config is
    needed.  Do not write these derived fields back into an existing plan.
    """
    if backend == 'topology_pair':
        from r2f.modules.s1.topology_targets import TOPOLOGY_CAPABILITY
        from r2f.modules.s1.topology_model import validate_program
        if runtime_capability != TOPOLOGY_CAPABILITY:
            raise ValueError('topology_runtime_capability_required')
        if not isinstance(final_input, dict) or set(final_input) != {'topology_program'}:
            raise ValueError('topology_final_input_keys_mismatch')
        validation = validate_program(final_input['topology_program'])
        if validation['status'] != 'valid' or validation['guard_disposition'] == 'guard_violation':
            raise ValueError('topology_runtime_input_not_applicable')
        return deepcopy(final_input)
    result = _controls(deepcopy(final_input), backend)
    if canonical_hash(result) != canonical_hash(final_input):
        raise ValueError('final_input controls are not canonical; normalize before composition')
    if backend == 'projection_pair':
        for role in ('writer', 'reader'):
            result[role + '_qos'] = deepcopy(final_input['qos'])
    return result


TOPOLOGY_SOURCE_SCHEMA = 'r2f.s1-seed-corpus/v2'


def validate_topology_profile(profile):
    """Validate the dedicated topology declaration, not permission to execute."""
    validate_search_profile(profile)
    from r2f.modules.s1.topology_targets import (
        TOPOLOGY_CAPABILITY, TOPOLOGY_TARGET, TOPOLOGY_SURFACE, TOPOLOGY_MUTATOR)
    from r2f.modules.s1 import fuzz_mutators_v2
    allowed = PROFILE_METADATA_FIELDS | {'source_schema', 'operator_whitelist', 'runtime_capability'}
    unknown = set(profile) - allowed
    if unknown:
        raise ValueError('unsupported topology profile field:' + ','.join(sorted(unknown)))
    if profile.get('runtime_capability') != TOPOLOGY_CAPABILITY:
        raise ValueError('topology_runtime_capability_required')
    required = {'schema_version': 'r2f.s1-fuzz-profile/v1', 'campaign_kind': 'exploratory',
                'activation': 'F2', 'enabled': False, 'required_catalog_included': False,
                'target_ids': [TOPOLOGY_TARGET], 'surfaces': [TOPOLOGY_SURFACE],
                'seed_catalog_status': 'authored_v2', 'source_schema': TOPOLOGY_SOURCE_SCHEMA,
                'mutator_whitelist': [TOPOLOGY_MUTATOR]}
    for key, value in required.items():
        if key not in profile or canonical_hash(profile[key]) != canonical_hash(value):
            raise ValueError('topology_profile_contract_mismatch:' + key)
    if not isinstance(profile.get('profile_id'), str) or not profile['profile_id']:
        raise ValueError('topology_profile_id_required')
    if type(profile.get('chain_length')) is not int or not 1 <= profile['chain_length'] <= 4:
        raise ValueError('topology_requires_chain_length_1_to_4')
    operators = profile.get('operator_whitelist')
    supported = {op for slot in fuzz_mutators_v2.surface_by_id('S1.topology').slots
                 for op in slot.operators}
    if not isinstance(operators, list) or not operators or any(
            not isinstance(op, str) or op not in supported for op in operators) or \
            len(set(operators)) != len(operators):
        raise ValueError('topology_operator_whitelist_invalid')
    rmws = profile.get('rmws')
    if not isinstance(rmws, list) or not rmws or any(
            rmw not in ('rmw_cyclonedds_cpp', 'rmw_fastrtps_cpp') for rmw in rmws) or \
            len(set(rmws)) != len(rmws):
        raise ValueError('topology_rmws_invalid')
    budget = profile.get('budget')
    if not isinstance(budget, dict) or set(budget) != {'max_inputs', 'max_seconds'} or \
            type(budget['max_inputs']) is not int or not 1 <= budget['max_inputs'] <= 256:
        raise ValueError('topology_bounded_budget_required')
    _number(budget['max_seconds'], 'topology.max_seconds', 0.001, 300)
    corpus = profile.get('corpus', {})
    if not isinstance(corpus, dict) or set(corpus) - {'capacity', 'bucket_limit'} or any(
            type(value) is not int or not 1 <= value <= 256 for value in corpus.values()):
        raise ValueError('topology_corpus_budget_invalid')
    cycles = profile.get('cycles')
    if not isinstance(cycles, list) or not cycles:
        raise ValueError('topology_explicit_cycles_required')
    ids, specs = set(), []
    for cycle in cycles:
        if not isinstance(cycle, dict) or set(cycle) != {
                'cycle_id', 'target_id', 'surfaces', 'seed_specs'}:
            raise ValueError('topology_cycle_shape_invalid')
        cycle_id = cycle['cycle_id']
        if not isinstance(cycle_id, str) or not cycle_id or cycle_id in ids:
            raise ValueError('topology_cycle_id_invalid_or_duplicate')
        ids.add(cycle_id)
        if cycle['target_id'] != TOPOLOGY_TARGET or cycle['surfaces'] != [TOPOLOGY_SURFACE]:
            raise ValueError('topology_cycle_target_or_surface_mismatch')
        if not isinstance(cycle['seed_specs'], list) or not cycle['seed_specs']:
            raise ValueError('topology_seed_specs_required')
        seen = set()
        for spec in cycle['seed_specs']:
            if not isinstance(spec, dict) or set(spec) != {
                    'seed_id', 'target_id', 'surface_id', 'mutator_id', 'boundary_class',
                    'expected_disposition', 'executable'}:
                raise ValueError('topology_seed_spec_shape_invalid')
            if not isinstance(spec['seed_id'], str) or not spec['seed_id'] or spec['seed_id'] in seen:
                raise ValueError('topology_seed_id_invalid_or_duplicate')
            seen.add(spec['seed_id'])
            if spec['target_id'] != TOPOLOGY_TARGET or spec['surface_id'] != TOPOLOGY_SURFACE or \
                    spec['mutator_id'] != TOPOLOGY_MUTATOR or spec['boundary_class'] != 'authored_topology':
                raise ValueError('topology_seed_spec_binding_mismatch')
            expected = spec['expected_disposition']
            if expected not in ('expect_pass', 'expect_guard_reject') or \
                    type(spec['executable']) is not bool or \
                    spec['executable'] is not (expected == 'expect_pass'):
                raise ValueError('topology_seed_guard_classification_mismatch')
            specs.append(spec)
    declared = profile.get('declared_seed_specs_union')
    union = {canonical_hash(spec): spec for spec in specs}
    if not isinstance(declared, list) or len(declared) != len(union) or \
            {canonical_hash(spec) for spec in declared} != set(union) or \
            profile.get('seed_specs') != declared:
        raise ValueError('topology_declared_cycle_seed_union_mismatch')
    return True
