"""Opt-in native initialization observations inside the existing topology worker.

Only a fresh, single-threaded worker may preload DDS and fork before the first
participant. Inheriting an initialized DDS stack is deliberately unsupported.
This module is a carrier boundary, never a runner or qualification authority.
"""
from __future__ import annotations

import ctypes
import json
import os
from pathlib import Path
import select
import signal
import socket
import time

SCHEMA = 'r2f.participant-identity-initialization/v1'
MODES = ('fresh_process', 'fork_before_participant')


def validate_identity_checks(program, config=None):
    if 'identity_checks' not in program:
        return None
    value = program['identity_checks']
    if (not isinstance(value, dict) or set(value) != {'initialization'}
            or value.get('initialization') not in MODES):
        raise ValueError('unsupported_initialization_contract:requires_fresh_process_or_fork_before_participant')
    if config is not None and (config.get('rmw') != 'rmw_cyclonedds_cpp'
            or config.get('observation_mode', 'blackbox') != 'blackbox'
            or any(config.get(k) is not None for k in
                   ('fastdds_peer_instrumentation', 'projection_sut_environment'))):
        raise ValueError('identity_checks_scope:installed_cyclone_blackbox_only')
    return value


def process_identity(pid=None):
    pid = os.getpid() if pid is None else pid
    stat = Path(f'/proc/{pid}/stat').read_text().rsplit(')', 1)[1].split()
    return {'pid': pid, 'ppid': int(stat[1]), 'pgid': int(stat[2]),
            'process_start_ticks': int(stat[19]),
            'pid_namespace': os.readlink(f'/proc/{pid}/ns/pid')}


def _send(sock, value):
    sock.sendall(json.dumps(value, sort_keys=True, allow_nan=False).encode() + b'\n')


def _receive(stream):
    line = stream.readline(2**20 + 1)
    if not line.endswith(b'\n') or len(line) > 2**20:
        raise ValueError('identity_probe_peer_frame_missing_or_oversized')
    return json.loads(line)


def _phases(domain, library_sha256, rendezvous):
    from .native_participant import NativeParticipantBackend
    from .topology_worker import CycloneMatchedPeerObserver
    phases = []
    for generation in (1, 2):
        backends = []
        phase = {'generation': generation, 'started_monotonic_ns': time.monotonic_ns(),
                 'participants': [], 'cleanup': [], 'cleanup_complete': False}
        phases.append(phase)
        try:
            for slot in range(2):
                backend = NativeParticipantBackend()
                backends.append(backend)
                row = {'slot': slot}
                phase['participants'].append(row)
                created = row['participant_created'] = backend.open({'enabled': True, 'domain_id': domain,
                    'live_authorization': {'granted': True, 'library_sha256': library_sha256}})
                if created.get('real_execution') is not True:
                    raise ValueError('identity_probe_participant_create_failed:' + str(created))
                reader = row['reader_created'] = backend.builtin_reader()
                if reader.get('real_execution') is not True:
                    raise ValueError('identity_probe_builtin_reader_create_failed:' + str(reader))
            observer = CycloneMatchedPeerObserver()
            for backend, row in zip(backends, phase['participants']):
                guid = observer._guid(row['reader_created']['reader_handle'])
                row['ownership'] = observer.sample_ownership(domain, guid, 'reader')
            phase['observed_monotonic_ns'] = time.monotonic_ns()
            # Both fork branches retain all entities until the other is observed.
            rendezvous(generation, phase)
        except (ValueError, TypeError, OSError, RuntimeError, AttributeError) as exc:
            phase['error'] = type(exc).__name__ + ':' + str(exc)
        finally:
            for backend in reversed(backends):
                phase['cleanup'].append(backend.close())
            phase['closed_monotonic_ns'] = time.monotonic_ns()
            phase['cleanup_complete'] = (len(backends) == 2
                and all(row.get('cleanup_complete') is True for row in phase['cleanup']))
        if 'error' in phase or not phase['cleanup_complete']:
            break
    return {'identity': process_identity(), 'phases': phases,
            'cleanup_complete': len(phases) == 2 and all(p['cleanup_complete'] for p in phases)}


def run_identity_probe(config):
    """Observe two participants/readers, full teardown/reinit, and optional fork.

    The canonical route owns environment/source/domain locks and kernel subtree
    cleanup. Every forked helper is additionally PIDFD-bound and reaped here.
    """
    contract = validate_identity_checks({'identity_checks': config['identity_checks']}, config)
    result = {'schema': SCHEMA, 'contract': dict(contract), 'status': 'unknown',
              'started_monotonic_ns': time.monotonic_ns(), 'processes': [],
              'cleanup_complete': False, 'candidate_eligible': False,
              'limitations': ['bounded_observations_not_exhaustive_uniqueness',
                  'iid_is_cyclone_entity_instance_id_not_portable_wire_field',
                  'fork_after_participant_unsupported', 'historical_pre_post_not_reproduced']}
    child = pidfd = parent = peer = stream = None
    reaped = False
    try:
        from .native_participant import load_library, VERSION_HEADER, _sha
        library, provenance = load_library()
        result['provenance'] = {**provenance, 'version_header': str(VERSION_HEADER),
                                'version_header_sha256': _sha(VERSION_HEADER),
                                'version_declaration': VERSION_HEADER.read_text()}
        lookup = library.dds_lookup_participant
        lookup.argtypes, lookup.restype = [ctypes.c_uint32, ctypes.c_void_p, ctypes.c_size_t], ctypes.c_int32
        count = lookup(config['domain_id'], None, 0)
        result['before_initialization'] = {'process': process_identity(), 'local_participant_count': count,
            'thread_count': len(list(Path('/proc/self/task').iterdir())), 'library_preloaded': True,
            'monotonic_ns': time.monotonic_ns()}
        if count != 0 or result['before_initialization']['thread_count'] != 1:
            raise ValueError('identity_probe_requires_fresh_single_threaded_process')
        if contract['initialization'] == 'fresh_process':
            result['processes'] = [_phases(config['domain_id'], provenance['library_sha256'], lambda *_: None)]
            result['cleanup_complete'] = result['processes'][0]['cleanup_complete']
        else:
            result['rendezvous'] = []
            result['peer_protocol'] = []
            parent, peer = socket.socketpair()
            parent.settimeout(8)
            peer.settimeout(8)
            child = os.fork()
            if child == 0:
                parent.close()
                stream = peer.makefile('rb')
                try:
                    if _receive(stream) != {'command': 'start'}:
                        raise ValueError('identity_probe_start_handshake_invalid')
                    def child_ready(generation, phase):
                        _send(peer, {'generation': generation, 'phase': phase, 'identity': process_identity()})
                        if _receive(stream) != {'release': generation}:
                            raise ValueError('identity_probe_release_handshake_invalid')
                    observed = _phases(config['domain_id'], provenance['library_sha256'], child_ready)
                    _send(peer, {'completed': observed})
                    os._exit(0 if observed['cleanup_complete'] else 1)
                except BaseException as exc:
                    try:
                        _send(peer, {'error': type(exc).__name__ + ':' + str(exc),
                                     'identity': process_identity()})
                    except BaseException:
                        pass
                    os._exit(2)
            peer.close()
            peer = None
            pidfd = os.pidfd_open(child)
            kernel_child = process_identity(child)
            if kernel_child['ppid'] != os.getpid():
                raise ValueError('identity_probe_child_parent_mismatch')
            result['child_kernel_identity'] = kernel_child
            stream = parent.makefile('rb')
            _send(parent, {'command': 'start'})
            def receive_child():
                packet = _receive(stream)
                result['peer_protocol'].append({'received_monotonic_ns': time.monotonic_ns(), 'packet': packet})
                if isinstance(packet, dict) and 'completed' in packet:
                    result['child_completion_packet'] = packet
                return packet
            def parent_ready(generation, phase):
                packet = receive_child()
                if packet.get('generation') != generation or not isinstance(packet.get('phase'), dict):
                    raise ValueError('identity_probe_phase_handshake_invalid')
                result['rendezvous'].append({'generation': generation, 'child_phase': packet['phase'],
                    'child_identity': packet.get('identity'),
                    'parent_observed_monotonic_ns': phase['observed_monotonic_ns'],
                    'received_monotonic_ns': result['peer_protocol'][-1]['received_monotonic_ns'],
                    'release_monotonic_ns': time.monotonic_ns()})
                _send(parent, {'release': generation})
            result['processes'].append(_phases(config['domain_id'], provenance['library_sha256'], parent_ready))
            packet = result.get('child_completion_packet')
            if packet is None:
                packet = receive_child()
            observed = packet.get('completed')
            if not isinstance(observed, dict) or observed.get('identity') != kernel_child:
                raise ValueError('identity_probe_child_kernel_binding_mismatch')
            result['processes'].append(observed)
            ready, _, _ = select.select([pidfd], [], [], 8)
            if not ready:
                raise ValueError('identity_probe_child_exit_timeout')
            _, status = os.waitpid(child, 0)
            reaped = True
            result['child_returncode'] = os.waitstatus_to_exitcode(status)
            result['cleanup_complete'] = result['child_returncode'] == 0 and all(
                p['cleanup_complete'] for p in result['processes'])
        if result['cleanup_complete'] and all(len(p['phases']) == 2 and not any(
                'error' in phase for phase in p['phases']) for p in result['processes']):
            result['status'] = 'observed'
    except (ValueError, TypeError, OSError, RuntimeError, AttributeError) as exc:
        result['reason'] = type(exc).__name__ + ':' + str(exc)
    finally:
        if child is not None and child > 0 and not reaped:
            if pidfd is not None:
                try:
                    signal.pidfd_send_signal(pidfd, signal.SIGKILL)
                except ProcessLookupError:
                    pass
            else:
                # An unreaped direct child cannot have its PID recycled.
                try:
                    os.kill(child, signal.SIGKILL)
                except ProcessLookupError:
                    pass
            os.waitpid(child, 0)
            reaped = True
            result['child_forced_cleanup'] = True
        if child is not None and child > 0:
            result['child_reaped'] = reaped
        for handle in (stream, parent, peer):
            if handle is not None:
                handle.close()
        if pidfd is not None:
            os.close(pidfd)
        result['ended_monotonic_ns'] = time.monotonic_ns()
    return result


def emit_identity_probe(config, clock):
    if 'identity_checks' not in config:
        return
    observation = run_identity_probe(config)
    identity = process_identity()
    row = {'event': 'participant_identity_initialization', **identity,
           'run_id': config['run_id'], 'domain_id': config['domain_id'],
           'time_ns': time.time_ns(), 'monotonic_ns': time.monotonic_ns(),
           'monotonic_clock_identity': clock, 'observation': observation}
    keys = ('group_id', 'worker_instance_id', 'context_instance_id')
    if 'participant_control' not in config:
        keys += ('logical_id', 'generation')
    for key in keys:
        if key in config:
            row[key] = config[key]
    print(json.dumps(row, sort_keys=True, allow_nan=False), flush=True)
    if not observation['cleanup_complete']:
        raise RuntimeError('identity_initialization_probe_cleanup_or_execution_failed')
