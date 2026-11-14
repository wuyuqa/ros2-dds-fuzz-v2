"""One bounded development probe, reusing the normal runtime endpoint worker.

Not a campaign entry point or seal. Domain ownership/preflight and raw evidence
are retained. Each of the two ROS processes has its own vendor output file.
"""
from __future__ import annotations
import json
import os
from pathlib import Path
import time
import uuid

from .cyclonedds import (RMW, build_trace_config, capture_process_provenance,
                         collect_trace, environment_for_mode, file_sha256, probe_cyclonedds)
from .projection import reconcile_projection


def _write(path, data):
    with Path(path).open('x') as stream:
        json.dump(data, stream, indent=2, sort_keys=True)
        stream.write('\n')


def run_development_probe(output_root, *, domain_id=174, timeout_sec=15):
    """Only call after coordinating this single normal ROS pair with its owner.

    Runtime/campaign can independently integrate the pure builder/collector APIs;
    this probe reuses WorkerPool unchanged and does not modify existing run output.
    """
    from r2f.runtime.domains import LeaseSet
    from r2f.runtime.environment import activated_environment, check_environment, REPO
    from r2f.runtime.process import WorkerPool
    from r2f.runtime.ros2_pair import normalize_config

    output_root = Path(output_root).resolve()
    output_root.mkdir(parents=True, exist_ok=True)
    run_id = 'vendor-dev-' + time.strftime('%Y%m%dT%H%M%S', time.gmtime()) + '-' + uuid.uuid4().hex[:8]
    output = output_root / run_id
    output.mkdir()
    config = normalize_config({'run_id': run_id, 'domain_id': domain_id, 'rmw': RMW,
                               'campaign_id': run_id, 'topic': '/r2f_vendor_development',
                               'source_repo': str(REPO), 'arm': 'baseline'})
    report = {'schema': 'r2f.instrumentation-development/v1', 'run_id': run_id,
              'domain_id': domain_id, 'rmw': RMW, 'development_only': True,
              'sealed': False, 'candidate_status': 'none', 'candidate_admission': 'not_performed',
              'started_at_unix_ns': time.time_ns(), 'output_dir': str(output), 'status': 'started',
              'mode': 'instrumentation', 'target_maturity': 'not_evaluated'}
    pools, events, captures, configs = {}, [], {}, {}
    raw_handle = (output / 'application-events.jsonl').open('x')

    def emit(name, **fields):
        event = {'event': name, 'observer_time_ns': time.time_ns(), **fields}
        events.append(event)
        raw_handle.write(json.dumps(event, sort_keys=True) + '\n')
        raw_handle.flush()

    def wait(predicate, label):
        until = time.monotonic() + timeout_sec
        while not predicate():
            for pool in pools.values():
                pool.pump(0.025)
                for role, worker in pool.workers.items():
                    if worker.poll() is not None:
                        raise RuntimeError(f'{role}_worker_exited:{worker.returncode}')
            if time.monotonic() >= until:
                raise TimeoutError(label)

    leases = LeaseSet(output_root / '.locks', output, run_id, domain_id)
    try:
        leases.__enter__()
        report['locks'] = list(leases.receipts)
        preflight = check_environment(config)
        report['preflight'] = preflight
        _write(output / 'preflight.json', preflight)
        if not preflight['ok']:
            raise RuntimeError(preflight['reason'])
        report['loader_probe'] = probe_cyclonedds()
        _, activated = activated_environment(config)
        # Baseline control is a task, not a fabricated completed blackbox run.
        clean = environment_for_mode({**activated, 'CYCLONEDDS_URI': '<stale instrumentation config>'}, mode='blackbox')
        report['blackbox_environment_check'] = {'CYCLONEDDS_URI_present': 'CYCLONEDDS_URI' in clean,
                                                  'mode': 'blackbox', 'executed_pair': False}
        report['control_task'] = {
            'status': 'not_run', 'kind': 'same_input_blackbox_comparison',
            'origin_run_id': run_id, 'mode': 'blackbox', 'rmw': RMW,
            'seed': 0, 'mutation_kind': None, 'arm': 'baseline',
            'precondition': 'normal_writer_reader_match', 'topic': config['topic'], 'qos': config['qos'],
            'compare': ['match', 'post_match_delivery', 'graph_endpoint_roles_and_qos'],
            'environment_requirement': 'strip CYCLONEDDS_URI; fresh public API ROS pair; fresh domain lease',
        }
        for role in ('writer', 'reader'):
            role_output = output / role
            role_output.mkdir()
            trace = build_trace_config(role_output / 'cyclonedds.log', domain_id=domain_id)
            configs[role] = trace
            _write(role_output / 'trace-config.json', trace.to_dict())
            env = environment_for_mode(activated, mode='instrumentation', trace_config=trace)
            pool = WorkerPool(config, env, role_output, emit)
            pools[role] = pool
            pool.start(role)
            wait(lambda: any(e['event'] == 'endpoint_created' and e.get('role') == role for e in events), role + '_created')
        wait(lambda: all(any(e['event'] == 'match' and e.get('role') == r and e.get('matched_count', 0) > 0 for e in events)
                         for r in ('writer', 'reader')), 'normal_pair_match')
        for role, pool in pools.items():
            captures[role] = capture_process_provenance(pool.workers[role].pid, trace_config=configs[role])
            _write(output / role / 'process-provenance.json', captures[role])
        for sequence in range(3):
            pools['writer'].command('writer', 'publish', send_sequence=sequence, phase='post_match')
            wait(lambda: any(e['event'] == 'receive' and e.get('send_sequence') == sequence for e in events), 'delivery')
        snapshot_start = len(events)
        pools['writer'].command('writer', 'snapshot')
        wait(lambda: any(e['event'] == 'graph_snapshot' and e.get('role') == 'writer' for e in events[snapshot_start:]), 'graph_snapshot')
        graph_raw = next(e for e in reversed(events) if e['event'] == 'graph_snapshot' and e.get('role') == 'writer')
        endpoints = []
        for key, kind in (('publishers', 'writer'), ('subscriptions', 'reader')):
            for entry in graph_raw[key]:
                endpoints.append({'topic_name': config['topic'], 'endpoint_kind': kind,
                                  'endpoint_gid': list(bytes.fromhex(entry['endpoint_gid'])),
                                  'qos': entry['qos'], 'ros_type': entry['topic_type']})
        snapshot = {'source': 'ros_graph', 'captured_at_unix_ns': graph_raw['time_ns'],
                    'api': 'get_publishers_info_by_topic/get_subscriptions_info_by_topic',
                    'endpoints': endpoints, 'application_event': graph_raw}
        _write(output / 'graph-snapshot.json', snapshot)
        snapshot.update(raw_path=str(output / 'graph-snapshot.json'), raw_sha256=file_sha256(output / 'graph-snapshot.json'))
        report['graph_snapshot'] = snapshot
        report['status'] = 'completed'
    except Exception as exc:
        report['status'] = 'failed'
        report['error'] = type(exc).__name__ + ':' + str(exc)
    finally:
        # Close owned workers before collecting; never signal preexisting processes.
        for pool in pools.values():
            try:
                pool.close()
            except Exception as exc:
                report.setdefault('cleanup_errors', []).append(type(exc).__name__ + ':' + str(exc))
                report['status'] = 'failed'
        raw_handle.close()
        leases.__exit__(None, None, None)
        report['processes'] = [metadata for pool in pools.values() for metadata in pool.metadata]
    traces = []
    for role, config_trace in configs.items():
        try:
            trace = collect_trace(config_trace, processes=[captures.get(role, {})], control_task=report.get('control_task', {'status': 'not_run'}))
            _write(output / role / 'vendor-observation.json', trace)
            traces.append(trace)
        except Exception as exc:
            report.setdefault('collection_errors', []).append(role + ':' + type(exc).__name__ + ':' + str(exc))
    report['internal_event_witness'] = len(traces) == len(configs) == 2 and all(t['internal_event_witness'] for t in traces)
    if report.get('collection_errors'):
        report['status'] = 'collection_failed'
    report['vendor_event_count'] = sum(len(t['events']) for t in traces)
    if 'graph_snapshot' in report:
        kwargs = dict(dds_topic='rt' + config['topic'], ros_topic=config['topic'], complete_window=True,
                      window_start_ns=report['started_at_unix_ns'], window_end_ns=time.time_ns())
        report['limited_projection'] = reconcile_projection(traces, report['graph_snapshot'], **kwargs)
        report['extended_projection'] = reconcile_projection(traces, report['graph_snapshot'],
                    required_qos=('reliability', 'durability', 'deadline', 'liveliness'), **kwargs)
    report['finished_at_unix_ns'] = time.time_ns()
    report['application_raw'] = {'path': str(output / 'application-events.jsonl'),
                                 'sha256': file_sha256(output / 'application-events.jsonl'),
                                 'source': 'rclpy_public_API_not_vendor_internal'}
    report['phase2_disposition'] = ('real_vendor_internal_events_observed_development_only'
                                    if report['internal_event_witness'] else 'not_yet_observed')
    _write(output / 'report.json', report)
    return report


def recollect_development_probe(run_dir):
    """Append a new derived analysis; preserve original raw/report and failures."""
    from .cyclonedds import TraceConfig
    origin = Path(run_dir).resolve()
    original = json.loads((origin / 'report.json').read_text())
    output = origin.parent / ('analysis-' + time.strftime('%Y%m%dT%H%M%S', time.gmtime()) + '-' + uuid.uuid4().hex[:8])
    output.mkdir()
    report = {'schema': 'r2f.instrumentation-reanalysis/v1', 'output_dir': str(output),
              'development_only': True, 'sealed': False, 'candidate_admission': 'not_performed',
              'origin_run_id': original['run_id'], 'origin_report': str(origin / 'report.json'),
              'origin_report_sha256': file_sha256(origin / 'report.json'),
              'reason': 'collector used atime in stable stat comparison; corrected parser on preserved raw',
              'original_collection_errors': original.get('collection_errors', []),
              'reexecuted_pair': False, 'domain_id': original['domain_id'], 'rmw': RMW,
              'parser_sha256': file_sha256(Path(__file__).with_name('cyclonedds.py')),
              'projection_sha256': file_sha256(Path(__file__).with_name('projection.py')),
              'control_task': original['control_task']}
    traces = []
    for role in ('writer', 'reader'):
        config = TraceConfig(**json.loads((origin / role / 'trace-config.json').read_text()))
        capture = json.loads((origin / role / 'process-provenance.json').read_text())
        observation = collect_trace(config, processes=[capture], control_task=original['control_task'])
        _write(output / (role + '-vendor-observation.json'), observation)
        traces.append(observation)
    report['internal_event_witness'] = all(t['internal_event_witness'] for t in traces)
    report['vendor_event_count'] = sum(len(t['events']) for t in traces)
    report['graph_snapshot'] = original['graph_snapshot']
    topic = report['control_task']['topic']
    kwargs = dict(dds_topic='rt' + topic, ros_topic=topic, complete_window=True,
                  window_start_ns=original['started_at_unix_ns'], window_end_ns=original['finished_at_unix_ns'])
    report['limited_projection'] = reconcile_projection(traces, report['graph_snapshot'], **kwargs)
    report['extended_projection'] = reconcile_projection(traces, report['graph_snapshot'],
                  required_qos=('reliability', 'durability', 'depth', 'deadline', 'liveliness'), **kwargs)
    report['phase2_disposition'] = 'real_vendor_internal_events_observed_development_only'
    _write(output / 'report.json', report)
    return report
