"""Topology bindings for the existing Episode executor and append-only store.

This is not another runner, oracle or candidate pipeline. Runtime supplies the
real action callback; consumers re-read immutable raw prefixes independently.
Checkpoint persistence is an observation cut, NOT DDS snapshot restoration.
"""
from copy import deepcopy
import hashlib
import json
from pathlib import Path

from r2f.core.evidence import canonical_hash
from r2f.core.topology_state import SCHEMA as STATE_SCHEMA
from r2f.core.state_signature import state_id
from .episode import (Episode, EpisodeAction, EpisodeLimits, EpisodeStore,
                      validate_reset_receipt, _durable_mkdir, _fsync_directory)
from .topology_state_projection import project_topology_state

PREFIX_SCHEMA = 'r2f.s1-topology-episode-prefix/v1'
AUDIT_SCHEMA = 'r2f.s1-topology-episode-audit/v1'


def topology_episode(program, config, initial_state):
    """Bind each existing program action to Episode, with no predicted effects."""
    digest = canonical_hash(program)
    target = config.get('target_id', 'discovery.endpoint_topology.fuzz_v1')
    revision = config.get('target_revision', 0)
    rmw = config.get('rmw', 'rmw_cyclonedds_cpp')
    contract = config.get('contract_hash') or digest
    indices = {a['action_id']: i for i, a in enumerate(program['actions'])}
    actions = [EpisodeAction(sequence=i, action=a['op'], action_id=a['action_id'],
        input_entry={'target_id': target, 'target_revision': revision, 'rmw': rmw,
                     'contract_hash': contract,
                     'final_config': {'program_sha256': digest, 'topology_action': deepcopy(a)}},
        parameters={'topology_action': deepcopy(a)},
        depends_on=[indices[d] for d in a.get('depends_on', [])])
        for i, a in enumerate(program['actions'])]
    return Episode(episode_id='topology-' + digest[:24], target_id=target,
        target_revision=revision, rmw=rmw, contract_hash=contract, actions=actions,
        initial_state=initial_state, state_schema=STATE_SCHEMA,
        limits=EpisodeLimits(max_actions=50, max_seconds=program['episode']['max_seconds']),
        isolation_mode='episode', tags={'program_sha256': digest,
            'checkpoint_action_ids': program['episode']['checkpoint_action_ids'],
            'reset_action_ids': program['episode'].get('reset_action_ids', []),
            'restoration_supported': False})


def capture_prefix(raw, raw_path, root, name, *, before_ref, after_ref, program):
    """Flush the live stream, then durably freeze precisely that prefix."""
    import os
    raw.flush()
    os.fsync(raw.fileno())
    content = Path(raw_path).read_bytes()
    path = Path(root) / 'prefixes' / (name + '.jsonl')
    _durable_mkdir(path.parent)
    with path.open('xb') as handle:
        handle.write(content)
        handle.flush()
        os.fsync(handle.fileno())
    _fsync_directory(path.parent)
    digest = hashlib.sha256(content).hexdigest()
    projection = project_topology_state(program, path, before_ref=before_ref,
        after_ref=after_ref, expected_sha256=digest)
    if projection['receipt']['status'] == 'invalid':
        raise ValueError('episode_state_prefix_invalid:' + ','.join(projection['receipt']['reasons']))
    return projection, {'schema': PREFIX_SCHEMA, 'path': str(path.resolve()),
        'sha256': digest, 'byte_count': len(content),
        'event_count': len(content.splitlines()), 'before_ref': before_ref,
        'after_ref': after_ref, 'projection': projection['receipt']}


def reset_from_events(events, action, prefix):
    """Recompute a fresh-runtime receipt from actual retirement and creation.

    Even a complete receipt claims only worker recreation, not baseline or
    distributed DDS memory restoration. Offline fixtures never qualify.
    """
    eid, generation = action['endpoint_id'], action['generation']
    observed = {'status': 'unknown', 'reason': 'reset_raw_lifecycle_incomplete',
                'baseline_restored': False, 'candidate_eligible': False}
    def one(kind, gen):
        rows = [e for e in events if e.get('event') == kind
                and e.get('logical_id') == eid and e.get('generation') == gen]
        return rows[0] if len(rows) == 1 else None
    previous, current = one('endpoint_created', generation-1), one('endpoint_created', generation)
    retired, exited = one('topology_endpoint_destroyed', generation-1), one('topology_worker_exit', generation-1)
    if any(e is None for e in (previous, current, retired, exited)):
        return observed
    lineage = ('logical_id', 'generation', 'pid', 'process_start_ticks', 'endpoint_generation')
    if (not all(all(e.get(k) == previous.get(k) and e.get(k) is not None for k in lineage)
                for e in (retired, exited))
            or retired.get('cleanup_status') != 'closed' or exited.get('returncode') != 0
            or not previous['event_index'] < exited['event_index'] <= retired['event_index'] < current['event_index']
            or any(current.get(k) in (None, '') for k in ('pid', 'process_start_ticks', 'endpoint_generation', 'endpoint_gid'))
            or (current['pid'], current['process_start_ticks']) == (previous['pid'], previous['process_start_ticks'])
            or not retired.get('endpoint_gid') or exited.get('endpoint_gid') != retired.get('endpoint_gid')
            or current.get('endpoint_gid') == retired.get('endpoint_gid')
            or any(e.get('backend') != 'ros2_public_api' or e.get('fixture') is True for e in
                   (previous, current, retired, exited))):
        return observed
    receipt = validate_reset_receipt({'schema': 'r2f.s1-reset-receipt/v1',
        'kind': 'runtime_recreated', 'cleanup_verified': True, 'resources_released': True,
        'baseline_restored': False, 'restored_state_id': None,
        'runtime': {k: current[k] for k in ('pid', 'generation', 'process_start_ticks', 'endpoint_gid', 'endpoint_generation')},
        'evidence': [{'path': prefix['path'], 'sha256': prefix['sha256'], 'event_index': e['event_index']}
                     for e in (previous, exited, retired, current)]},
        previous_runtime={'pid': previous['pid'], 'generation': previous['generation']})
    return {'status': 'observed', 'receipt': receipt, 'baseline_restored': False,
            'candidate_eligible': False}


def audit_topology_episode(program, runtime, config):
    """Consume raw, prefixes and store records, never runtime episode summaries.

    Structural persistence, real execution, observation quality and restoration
    are separate results. This consumer does not issue a DDS oracle pass.
    """
    result = {'schema': AUDIT_SCHEMA, 'status': 'incomplete', 'structural_valid': False,
              'completed': False, 'execution_evidence': False, 'fixture': True,
              'checkpoint_observations_complete': False, 'reset_verified': False,
              'restoration_supported': False, 'candidate_eligible': False,
              'reasons': [], 'steps': []}
    if 'episode' not in program:
        return dict(result, status='not_applicable')
    def require(condition, reason):
        if not condition:
            raise ValueError(reason)
    try:
        path = Path(runtime['raw_path']).resolve()
        content = path.read_bytes()
        require(hashlib.sha256(content).hexdigest() == runtime['raw_sha256'], 'episode_raw_hash_mismatch')
        lines = content.splitlines(keepends=True)
        events = [json.loads(line) for line in lines]
        require(all(e.get('event_index') == i for i, e in enumerate(events)), 'episode_raw_indices_invalid')
        def one(kind):
            rows = [e for e in events if e.get('event') == kind]
            require(len(rows) == 1, 'episode_event_missing_or_duplicate:' + kind)
            return rows[0]
        begin, end = one('topology_episode_begin'), one('topology_episode_end')
        require(begin['event_index'] < end['event_index'], 'episode_begin_end_order')
        root = path.parent / 'episode'
        require(Path(end['store_root']).resolve() == root, 'episode_store_path_mismatch')

        def project(prefix, cut_index):
            prefix_path = Path(prefix['path']).resolve()
            require(prefix_path.is_relative_to(root / 'prefixes'), 'episode_prefix_path_outside_store')
            require(prefix.get('schema') == PREFIX_SCHEMA, 'episode_prefix_schema')
            require(type(prefix.get('event_count')) is int and prefix['event_count'] == cut_index,
                    'episode_prefix_not_precommit_cut')
            data = prefix_path.read_bytes()
            require(data == b''.join(lines[:cut_index]) and prefix['byte_count'] == len(data),
                    'episode_prefix_not_exact_raw_prefix')
            require(hashlib.sha256(data).hexdigest() == prefix['sha256'], 'episode_prefix_hash')
            require(prefix['after_ref'] == cut_index-1, 'episode_boundary_not_prefix_end')
            projected = project_topology_state(program, prefix_path, before_ref=prefix['before_ref'],
                after_ref=prefix['after_ref'], expected_sha256=prefix['sha256'])
            require(projected['receipt']['status'] != 'invalid', 'episode_prefix_projection_invalid')
            require(projected['receipt'] == prefix['projection'], 'episode_projection_receipt_mismatch')
            return projected

        initial = project(begin['initial_prefix'], begin['event_index'])
        require(events[begin['initial_prefix']['before_ref']].get('phase') == 'before_program'
                and events[begin['initial_prefix']['after_ref']].get('phase') == 'episode_initial',
                'episode_initial_boundary_binding')
        episode = topology_episode(program, config, initial['state_after'])
        require(begin['episode_id'] == end['episode_id'] == episode.episode_id
                and begin['input_sha256'] == end['input_sha256'] == episode.input_identity(),
                'episode_input_binding_mismatch')
        loaded = EpisodeStore(root).load(episode.episode_id)
        require(loaded['episode'].to_dict() == episode.to_dict(), 'episode_stored_definition_mismatch')
        require(loaded['record_sha256'] == end['record_sha256'], 'episode_end_store_hash_mismatch')
        execution = loaded['execution']
        receipts = execution['receipts']
        commits = [e for e in events if e.get('event') == 'topology_episode_action_committed']
        require(len(commits) == len(receipts) == execution['action_count'] == end['action_count'],
                'episode_action_count_mismatch')
        require(execution['stop_reason'] == end['stop_reason'], 'episode_stop_reason_mismatch')
        require(len(receipts) <= len(episode.actions), 'episode_extra_actions')
        previous_boundary, previous_state = begin['initial_prefix']['after_ref'], initial['state_after']
        previous_commit = begin['event_index']
        all_real = initial['receipt']['execution_evidence']
        checkpoints, resets = [], []
        for index, (receipt, commit) in enumerate(zip(receipts, commits)):
            action = program['actions'][index]
            aid = action['action_id']
            require(commit['episode_id'] == episode.episode_id and commit['action_id'] == aid
                    and commit['sequence'] == receipt['sequence'] == index
                    and receipt['action_id'] == aid
                    and receipt['input_sha256'] == episode.actions[index].input_sha256
                    and commit['receipt_sha256'] == canonical_hash(receipt), 'episode_action_binding_mismatch')
            require(receipt['oracle_verdict'] == 'unknown', 'episode_cannot_claim_oracle_pass')
            starts = [e for e in events if e.get('event') == 'topology_action_begin' and e.get('action_id') == aid]
            dones = [e for e in events if e.get('event') == 'topology_action_done' and e.get('action_id') == aid]
            require(len(starts) == len(dones) == 1, 'episode_actual_action_missing_or_duplicate')
            start, done = starts[0], dones[0]
            observation = receipt['observation']
            require(previous_commit < start['event_index'] < done['event_index'] < commit['event_index'] < end['event_index'],
                    'episode_action_commit_chronology_invalid')
            require(start['action'] == action and observation['action_done_ref'] == done['event_index']
                    and observation['runtime_action_receipt'] == done['receipt'], 'episode_raw_action_receipt_mismatch')
            rejected = done['receipt'].get('disposition') in {
                'guard_reject', 'capability_reject', 'tool_error', 'environment_unverified'}
            require(receipt['action'] == action['op']
                    and receipt['parameters'] == episode.actions[index].parameters
                    and receipt['status'] == ('failed' if rejected else 'executed')
                    and receipt['execution_stage'] == ('aborted' if rejected else 'effect_observed'),
                    'episode_action_disposition_mismatch')
            prefix = observation['prefix']
            require(prefix['before_ref'] == previous_boundary, 'episode_boundary_chain_mismatch')
            projected = project(prefix, commit['event_index'])
            require(events[prefix['after_ref']].get('phase') == 'episode_after:' + aid,
                    'episode_step_boundary_binding')
            require(receipt['before_state'] == projected['state_before'] == previous_state
                    and receipt['after_state'] == projected['state_after'], 'episode_state_chain_mismatch')
            require(receipt['before_state_id'] == state_id(projected['state_before'], schema=STATE_SCHEMA)
                    and receipt['after_state_id'] == state_id(projected['state_after'], schema=STATE_SCHEMA),
                    'episode_state_id_mismatch')
            require(observation['observation_complete'] is
                    (projected['state_after']['observation_completeness'] == 'complete')
                    and observation['execution_evidence'] is projected['receipt']['execution_evidence'],
                    'episode_observation_quality_mismatch')
            all_real = all_real and projected['receipt']['execution_evidence']
            checkpoint = aid in program['episode']['checkpoint_action_ids']
            require(commit['checkpoint'] is checkpoint, 'episode_checkpoint_declaration_mismatch')
            progress_path = root / 'checkpoints' / episode.episode_id / (f'{index:04d}.json')
            require(Path(commit['progress']['path']).resolve() == progress_path, 'episode_progress_path_mismatch')
            progress_bytes = progress_path.read_bytes()
            require(hashlib.sha256(progress_bytes).hexdigest() == commit['progress']['sha256'], 'episode_progress_file_hash')
            progress = json.loads(progress_bytes)
            require(progress['record_sha256'] == commit['progress']['record_sha256'] == canonical_hash(
                {k: v for k, v in progress.items() if k != 'record_sha256'}), 'episode_progress_record_hash')
            require(progress == dict(schema='r2f.s1-episode-progress/v1', episode_id=episode.episode_id,
                input_sha256=episode.input_identity(), state_schema=episode.state_schema,
                action_count=index+1, checkpoint=checkpoint, receipts=receipts[:index+1],
                restoration_supported=False, record_sha256=progress['record_sha256']), 'episode_progress_prefix_mismatch')
            if checkpoint:
                checkpoints.append(projected['state_after']['observation_completeness'] == 'complete')
            if aid in program['episode'].get('reset_action_ids', []):
                reset = reset_from_events(events[:prefix['event_count']], action, prefix)
                require(observation.get('reset') == reset, 'episode_reset_raw_mismatch')
                resets.append(reset['status'] == 'observed')
            previous_boundary, previous_state = prefix['after_ref'], projected['state_after']
            previous_commit = commit['event_index']
            result['steps'].append({'action_id': aid, 'sequence': index, 'commit_ref': commit['event_index'],
                'prefix_sha256': prefix['sha256'], 'checkpoint': checkpoint,
                'observation_complete': projected['state_after']['observation_completeness'] == 'complete'})
        result.update(structural_valid=True, execution_evidence=bool(all_real), fixture=not all_real,
            completed=execution['stop_reason'] == 'episode_completed' and len(receipts) == len(episode.actions),
            checkpoint_observations_complete=len(checkpoints) == len(program['episode']['checkpoint_action_ids']) and all(checkpoints),
            reset_verified=len(resets) == len(program['episode'].get('reset_action_ids', [])) and all(resets),
            stop_reason=execution['stop_reason'], action_count=len(receipts),
            episode_input_sha256=episode.input_identity(), record_sha256=loaded['record_sha256'])
        from .topology_episode_rebuild import audit_rebuild
        rebuilt = audit_rebuild(program, runtime, config, execution)
        if rebuilt is not None:
            result['checkpoint_reconstruction'] = rebuilt
            if result['completed']:
                require(rebuilt['validated'], 'episode_rebuild_not_validated')
                if result['execution_evidence']:
                    require(rebuilt['execution_evidence'] is True, 'episode_rebuild_nonreal_origin')
        if result['completed'] and result['checkpoint_observations_complete'] and result['reset_verified']:
            result['status'] = 'observed'
    except (OSError, ValueError, TypeError, KeyError, IndexError, UnicodeError) as exc:
        # A late dependency/reconstruction failure must revoke provisional
        # qualification, while retaining the diagnostic step history.
        result.update(status='invalid', structural_valid=False, completed=False,
                      execution_evidence=False, checkpoint_observations_complete=False,
                      reset_verified=False, candidate_eligible=False)
        result['reasons'].append(str(exc))
    return result
