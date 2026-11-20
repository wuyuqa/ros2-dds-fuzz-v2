"""Reconstruct an observed Episode checkpoint in a fresh public-API runtime.

A reconstruction executes the original prefix. Only an equal, complete typed
checkpoint permits suffix actions. It never restores DDS memory or asserts
hidden-state equivalence, and never promotes a historical unknown into pass.
"""
from copy import deepcopy
import hashlib
import json
from pathlib import Path

from r2f.core.evidence import canonical_hash
from r2f.core.state_signature import state_id
from r2f.core.topology_state import SCHEMA as STATE_SCHEMA
from .episode import EpisodeStore
from .topology_episode import topology_episode
from .topology_state_projection import project_topology_state

SCHEMA = 'r2f.s1-episode-rebuild-request/v1'
MODE = 'public_api_prefix_rebuild_then_suffix'


def _require(condition, reason):
    if not condition:
        raise ValueError('episode_rebuild:' + reason)


def _read(path):
    path = Path(path).resolve()
    data = path.read_bytes()
    return json.loads(data), {'path': str(path), 'sha256': hashlib.sha256(data).hexdigest()}


def checkpoint_request(config_path, runtime_path, action_id):
    """Freeze origin references; called by existing replay, not a new runner."""
    config, cfg_ref = _read(config_path)
    runtime, run_ref = _read(runtime_path)
    program = config.get('topology_program', {})
    _require('episode' in program, 'episode_required')
    actions = program['actions']
    matches = [i for i, a in enumerate(actions) if a['action_id'] == action_id]
    _require(len(matches) == 1 and action_id in program['episode']['checkpoint_action_ids'], 'checkpoint_not_declared')
    sequence = matches[0]
    _require(sequence < len(actions)-1, 'checkpoint_has_no_suffix')
    raw = Path(runtime['raw_path']).resolve()
    _require(raw.is_relative_to(Path(runtime_path).resolve().parent), 'origin_raw_outside_runtime')
    begins = [e for e in runtime['events'] if e.get('event') == 'topology_episode_begin']
    _require(len(begins) == 1, 'origin_episode_begin_missing')
    progress = raw.parent/'episode'/'checkpoints'/begins[0]['episode_id']/(f'{sequence:04d}.json')
    _, progress_ref = _read(progress)
    return {'schema': SCHEMA, 'mode': MODE, 'checkpoint_action_id': action_id,
            'sequence': sequence, 'origin_config': cfg_ref, 'origin_runtime': run_ref,
            'progress': progress_ref}


def load_checkpoint(request, program, config):
    """Revalidate the original persisted prefix, identities and successful cleanup.

    The immutable progress loader does not require an Episode final record.
    An origin may have stopped AFTER this checkpoint; the checkpoint itself
    must have committed before any tool failure and all origin workers closed.
    """
    _require(isinstance(request, dict) and set(request) == {
        'schema', 'mode', 'checkpoint_action_id', 'sequence', 'origin_config', 'origin_runtime', 'progress'},
        'request_fields')
    _require(request['schema'] == SCHEMA and request['mode'] == MODE, 'request_schema_or_mode')
    origin, current = {}, config
    for name in ('origin_config', 'origin_runtime', 'progress'):
        reference = request[name]
        _require(isinstance(reference, dict) and set(reference) == {'path', 'sha256'}, 'reference_fields')
        value, measured = _read(reference['path'])
        _require(measured == reference, 'reference_hash_mismatch:' + name)
        origin[name] = value
    old_config, runtime = origin['origin_config'], origin['origin_runtime']
    _require('episode_rebuild' not in old_config, 'recursive_checkpoint_origin_not_supported')
    for key in ('source_manifest', 'target_id', 'target_revision', 'rmw', 'arm',
                'contract_hash', 'effective_input_sha256', 'topology_program'):
        _require(canonical_hash(old_config.get(key)) == canonical_hash(current.get(key)), 'origin_input_mismatch:' + key)
    _require(old_config.get('topology_program') == program, 'origin_program_mismatch')
    raw_path = Path(runtime['raw_path']).resolve()
    _require(raw_path.is_relative_to(Path(request['origin_runtime']['path']).resolve().parent), 'origin_raw_outside_runtime')
    content = raw_path.read_bytes()
    _require(hashlib.sha256(content).hexdigest() == runtime['raw_sha256'], 'origin_raw_hash')
    events = [json.loads(line) for line in content.splitlines()]
    _require(events == runtime['events'] and all(e.get('event_index') == i for i, e in enumerate(events)), 'origin_raw_events')
    ends = [e for e in events if e.get('event') == 'runtime_end']
    _require(len(ends) == 1 and ends[0].get('cleanup_complete') is True, 'origin_cleanup_not_verified')
    # A summary closed=true is not worker cleanup proof. Every allocated
    # incarnation must have its own actual exit, even if a later action failed.
    from r2f.runtime.endpoint_incarnation import endpoint_identity_at
    for token in (e for e in events if e.get('event') == 'endpoint_created'):
        exits = [e for e in events if e.get('event') == 'topology_worker_exit'
                 and e.get('logical_id') == token.get('logical_id')
                 and e.get('generation') == token.get('generation')]
        _require(len(exits) == 1 and type(exits[0].get('returncode')) is int
                 and token['event_index'] < exits[0]['event_index'] < ends[0]['event_index'],
                 'origin_worker_exit_missing_or_ambiguous')
        identity = endpoint_identity_at(events, token, before_index=exits[0]['event_index']+1)
        _require(all(exits[0].get(k) == identity.get(k) for k in
            ('logical_id', 'generation', 'pid', 'process_start_ticks', 'endpoint_generation', 'endpoint_gid')),
            'origin_worker_exit_identity_mismatch')
    begins = [e for e in events if e.get('event') == 'topology_episode_begin']
    _require(len(begins) == 1, 'origin_episode_begin_missing')
    begin = begins[0]
    root = raw_path.parent/'episode'

    def project(prefix, cut_index):
        path = Path(prefix['path']).resolve()
        _require(path.is_relative_to(root/'prefixes'), 'prefix_outside_origin_store')
        data = path.read_bytes()
        _require(prefix['event_count'] == cut_index and prefix['after_ref'] == cut_index-1
                 and data == b''.join(content.splitlines(keepends=True)[:cut_index])
                 and prefix['byte_count'] == len(data)
                 and hashlib.sha256(data).hexdigest() == prefix['sha256'], 'origin_prefix_binding')
        projected = project_topology_state(program, path, before_ref=prefix['before_ref'],
            after_ref=prefix['after_ref'], expected_sha256=prefix['sha256'])
        _require(projected['receipt']['status'] != 'invalid'
                 and projected['receipt'] == prefix['projection'], 'origin_prefix_projection')
        return projected

    initial = project(begin['initial_prefix'], begin['event_index'])
    episode = topology_episode(program, current, initial['state_after'])
    sequence = request['sequence']
    _require(type(sequence) is int and 0 <= sequence < len(episode.actions)-1, 'checkpoint_sequence_invalid')
    progress = EpisodeStore(root).load_progress(episode, sequence)
    _require(progress == origin['progress'] and progress['checkpoint'] is True, 'origin_checkpoint_record')
    _require(request['checkpoint_action_id'] == episode.actions[sequence].action_id
             and request['checkpoint_action_id'] in program['episode']['checkpoint_action_ids']
             and sequence < len(episode.actions)-1, 'origin_checkpoint_action')
    _require(Path(request['progress']['path']).resolve() == root/'checkpoints'/episode.episode_id/(f'{sequence:04d}.json'),
             'origin_progress_path')
    previous, previous_commit = begin['initial_prefix']['after_ref'], begin['event_index']
    checkpoints = []
    for index, receipt in enumerate(progress['receipts']):
        aid = receipt['action_id']
        commits = [e for e in events if e.get('event') == 'topology_episode_action_committed' and e.get('action_id') == aid]
        starts = [e for e in events if e.get('event') == 'topology_action_begin' and e.get('action_id') == aid]
        dones = [e for e in events if e.get('event') == 'topology_action_done' and e.get('action_id') == aid]
        _require(len(commits) == len(starts) == len(dones) == 1, 'origin_action_trace_missing_or_duplicate')
        commit, start, done = commits[0], starts[0], dones[0]
        _require(previous_commit < start['event_index'] < done['event_index'] < commit['event_index'], 'origin_action_chronology')
        observation = receipt['observation']
        _require(commit['receipt_sha256'] == canonical_hash(receipt)
                 and commit['episode_id'] == episode.episode_id
                 and commit['sequence'] == index
                 and start['action'] == program['actions'][index]
                 and observation['action_done_ref'] == done['event_index']
                 and observation['runtime_action_receipt'] == done['receipt']
                 and receipt['execution_stage'] == 'effect_observed' and receipt['status'] == 'executed'
                 and done['receipt'].get('satisfied') is True,
                 'origin_action_not_successfully_observed')
        prefix = observation['prefix']
        _require(prefix['before_ref'] == previous, 'origin_boundary_chain')
        projected = project(prefix, commit['event_index'])
        _require(receipt['before_state'] == projected['state_before']
                 and receipt['after_state'] == projected['state_after'], 'origin_receipt_projection_mismatch')
        previous, previous_commit = prefix['after_ref'], commit['event_index']
        checkpoints.append(projected)
    selected = checkpoints[-1]
    bindings = [e for e in events[:previous_commit+1] if e.get('event') == 'input_binding']
    _require(len(bindings) == 1 and bindings[0].get('receipt', {}).get('program_sha256') == canonical_hash(program),
             'origin_raw_input_binding')
    if selected['receipt']['execution_evidence']:
        sources = [e for e in events if e.get('event') == 'topology_source_checked']
        _require(config.get('source_manifest', {}).get('source_frozen') is True
                 and sources and all(e.get('source') == config['source_manifest'] for e in sources),
                 'origin_source_provenance')
        phases = {e.get('phase') for e in sources if e['event_index'] < previous_commit}
        _require({'before_preflight', 'after_preflight', 'before_presteps'} <= phases
                 and all('before_action:' + a.action_id in phases for a in episode.actions[:sequence+1]),
                 'origin_source_checks_missing')
        envs = [e for e in events if e.get('event') == 'environment_preflight']
        locks = [e for e in events if e.get('event') == 'isolation_locks_acquired']
        releases = [e for e in events if e.get('event') == 'isolation_locks_released']
        _require(len(envs) == len(locks) == len(releases) == 1 and envs[0].get('ok') is True
                 and locks[0]['event_index'] < begin['event_index']
                 and previous_commit < releases[0]['event_index'], 'origin_preflight_or_cleanup_provenance')
    _require(selected['state_after']['observation_completeness'] == 'complete', 'origin_checkpoint_incomplete')
    _require(not any(e.get('event') in {'runtime_abort', 'worker_error', 'worker_command_error',
                 'worker_unexpected_exit', 'topology_tool_error'} for e in events[:previous_commit+1]),
             'origin_failure_before_checkpoint')
    _require(runtime.get('backend') == config.get('backend', runtime.get('backend')), 'origin_backend_mismatch')
    return {'request': deepcopy(request), 'episode': episode, 'progress': progress,
        'checkpoint_state': selected['state_after'], 'checkpoint_commit_ref': previous_commit,
        'origin_raw': {'path': str(raw_path), 'sha256': runtime['raw_sha256']},
        'origin_events': events, 'execution_evidence': selected['receipt']['execution_evidence'],
        'scope': 'projected_checkpoint_reconstruction_not_DDS_memory_restoration'}


def validate_reconstructed_checkpoint(loaded, receipt, events):
    """Validate the reconstructed cut, including fresh endpoint incarnations."""
    _require(receipt['sequence'] == loaded['request']['sequence'], 'rebuild_checkpoint_sequence')
    expected = loaded['checkpoint_state']
    actual = receipt['after_state']
    _require(actual == expected and actual['observation_completeness'] == 'complete', 'rebuild_checkpoint_state_mismatch')
    prefix = receipt['observation']['prefix']
    new_real = receipt['observation']['execution_evidence'] is True
    origin_real = loaded['execution_evidence'] is True
    _require(new_real == origin_real, 'checkpoint_execution_provenance_mismatch')
    real = new_real and origin_real
    old_prefix = loaded['progress']['receipts'][-1]['observation']['prefix']
    old = loaded['origin_events'][:old_prefix['event_count']]
    fresh = events[:prefix['event_count']]
    identities = []
    for endpoint in actual['endpoints']:
        if endpoint['liveness'] != 'alive':
            continue
        eid, gen = endpoint['logical_id'], endpoint['generation']
        def created(rows):
            matches = [e for e in rows if e.get('event') == 'endpoint_created'
                       and e.get('logical_id') == eid and e.get('generation') == gen]
            _require(len(matches) == 1, 'checkpoint_endpoint_creation_missing')
            return matches[0]
        prior, current = created(old), created(fresh)
        if real:
            _require((prior['pid'], prior['process_start_ticks']) != (current['pid'], current['process_start_ticks'])
                     and prior['endpoint_gid'] != current['endpoint_gid'], 'rebuild_endpoint_not_fresh')
        identities.append({'logical_id': eid, 'generation': gen,
            'origin_creation_ref': prior['event_index'], 'new_creation_ref': current['event_index'],
            'origin_pid': prior['pid'], 'new_pid': current['pid'],
            'origin_start_ticks': prior['process_start_ticks'], 'new_start_ticks': current['process_start_ticks'],
            'origin_gid': prior['endpoint_gid'], 'new_gid': current['endpoint_gid']})
    return {'schema': 'r2f.s1-episode-rebuild-validation/v1', 'mode': MODE,
        'checkpoint_action_id': receipt['action_id'], 'sequence': receipt['sequence'],
        'checkpoint_state_id': state_id(actual, schema=STATE_SCHEMA),
        'origin_progress': deepcopy(loaded['request']['progress']),
        'origin_raw': deepcopy(loaded['origin_raw']), 'checkpoint_prefix': deepcopy(prefix),
        'endpoint_reconstruction': identities, 'state_match': True,
        'execution_evidence': real, 'fixture': not real, 'candidate_eligible': False,
        'DDS_memory_restored': False, 'hidden_state_equivalence_claimed': False}


def audit_rebuild(program, runtime, config, execution):
    """Independently bind a request and gate event to actual new-run chronology."""
    events = runtime['events']
    begins = [e for e in events if e.get('event') == 'topology_episode_rebuild_begin']
    validations = [e for e in events if e.get('event') == 'topology_episode_rebuild_validated']
    requested = 'episode_rebuild' in config
    if not requested:
        _require(not begins and not validations, 'unrequested_rebuild_evidence')
        return None
    loaded = load_checkpoint(config['episode_rebuild'], program, config)
    _require(len(begins) == 1 and begins[0].get('request') == config['episode_rebuild']
             and begins[0].get('origin_raw') == loaded['origin_raw']
             and begins[0].get('mode') == MODE and begins[0].get('DDS_memory_restored') is False,
             'rebuild_begin_binding')
    seq = loaded['request']['sequence']
    starts = [e for e in events if e.get('event') == 'topology_action_begin']
    _require(not starts or begins[0]['event_index'] < starts[0]['event_index'], 'rebuild_begin_after_action')
    if len(execution['receipts']) <= seq or len(validations) != 1:
        _require(execution['stop_reason'] != 'episode_completed', 'completed_without_rebuild_validation')
        _require(not any(e.get('action_index', -1) > seq for e in starts), 'suffix_before_rebuild_validation')
        return {'status': 'incomplete', 'validated': False, 'mode': MODE, 'execution_evidence': False,
                'DDS_memory_restored': False, 'hidden_state_equivalence_claimed': False}
    receipt = execution['receipts'][seq]
    validation = validations[0]
    expected = validate_reconstructed_checkpoint(loaded, receipt, events)
    commits = [e for e in events if e.get('event') == 'topology_episode_action_committed'
               and e.get('sequence') == seq]
    _require(len(commits) == 1 and validation['checkpoint_commit_ref'] == commits[0]['event_index']
             and validation['validation'] == expected
             and commits[0]['event_index'] < validation['event_index'], 'rebuild_validation_mismatch')
    _require(all(e['event_index'] > validation['event_index'] for e in starts if e['action_index'] > seq),
             'suffix_before_rebuild_validation')
    return {'status': 'observed', 'validated': True, 'mode': MODE,
            'validation_ref': validation['event_index'], 'checkpoint_action_id': receipt['action_id'],
            'checkpoint_state_id': expected['checkpoint_state_id'],
            'prefix_actions_reexecuted': seq+1, 'suffix_actions_executed': len(execution['receipts'])-seq-1,
            'execution_evidence': expected['execution_evidence'], 'fixture': expected['fixture'],
            'DDS_memory_restored': False, 'hidden_state_equivalence_claimed': False}
