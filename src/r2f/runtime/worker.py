"""Fresh-process public rclpy endpoint; stdin commands, stdout JSONL tokens only."""
from __future__ import annotations
import json
import os
from pathlib import Path
import select
import sys
import time
import traceback
import uuid


def emit_snapshot(endpoint, role, command_id, endpoint_gid, graph, emit):
    """Read public match count afresh for every requested snapshot.

    Discovery can notify the writer before create_subscription() returns in the
    reader process. A later explicit query establishes a comparable observation
    without changing the original count-transition timestamp.
    """
    api = ('get_subscription_count' if role in ('writer', 'writer2')
           else 'get_publisher_count')
    sampled_ns = time.monotonic_ns()
    count = getattr(endpoint, api)()
    emit('match_snapshot', command_id=command_id, matched_count=count,
         endpoint_gid=endpoint_gid, sampled_monotonic_ns=sampled_ns, count_api=api)
    emit('graph_snapshot', command_id=command_id, **graph())


def extended_graph_qos(qos, enabled=False):
    """Additional public graph fields are opt-in; first two target tokens stay stable."""
    if not enabled:
        return {}
    return {'history': qos.history.name.lower(),
            'deadline_ns': qos.deadline.nanoseconds,
            'liveliness': qos.liveliness.name.lower(),
            'liveliness_lease_duration_ns': qos.liveliness_lease_duration.nanoseconds}


PUBLIC_QOS_FIELDS = frozenset({'depth', 'history', 'reliability', 'durability',
                                'deadline_ns', 'liveliness', 'liveliness_lease_duration_ns'})


def create_public_qos(q):
    """Construct only public rclpy policies, rejecting fields it cannot express.

    Unknown fields are an adapter error, never silently dropped: the caller's
    binding check (V35) must be able to see that the submitted input did not
    reach the public API.
    """
    unknown = set(q) - PUBLIC_QOS_FIELDS
    if unknown:
        raise ValueError('unsupported_public_qos_field:' + ','.join(sorted(unknown)))
    history_name = q.get('history', 'keep_last')
    if history_name != 'keep_last':
        # This runtime always requests KEEP_LAST; accepting anything else here
        # would be an unverifiable silent normalization.
        raise ValueError('unsupported_public_history:' + str(history_name))
    from rclpy.qos import QoSProfile, ReliabilityPolicy, DurabilityPolicy, HistoryPolicy, LivelinessPolicy
    from rclpy.duration import Duration
    kwargs = {'depth': q['depth'], 'history': HistoryPolicy.KEEP_LAST,
              'reliability': getattr(ReliabilityPolicy, q['reliability'].upper()),
              'durability': getattr(DurabilityPolicy, q['durability'].upper())}
    for key, policy in (('deadline_ns', 'deadline'), ('liveliness_lease_duration_ns', 'liveliness_lease_duration')):
        if key in q:
            kwargs[policy] = Duration(nanoseconds=q[key])
    if 'liveliness' in q:
        kwargs['liveliness'] = getattr(LivelinessPolicy, q['liveliness'].upper())
    return QoSProfile(**kwargs)


def self_qos_entry(graph, role, endpoint_gid):
    """Return the SDK-reported graph entry for this worker's own endpoint.

    The publisher/subscription choice follows the endpoint *kind*, not a fixed
    role list: writer2..writer4 (and any future writerN) publish, while
    readerN subscribes.  Hard-coding writer/writer2 sent writer3 to the
    subscription table, so its binding observation was always empty.
    """
    is_publisher = role.startswith('writer') or role == 'probe'
    rows = graph['publishers'] if is_publisher else graph['subscriptions']
    for item in rows:
        if item.get('endpoint_gid') == endpoint_gid:
            return item
    return None


def main():
    config = json.loads(sys.argv[1])
    role = sys.argv[2]
    # Imports are deliberately confined to this fresh child.
    import rclpy
    from rclpy.context import Context
    from rclpy.executors import SingleThreadedExecutor
    from rclpy.node import Node
    from rclpy.qos import QoSProfile, ReliabilityPolicy, DurabilityPolicy, HistoryPolicy
    from std_msgs.msg import String
    context = Context()
    rclpy.init(context=context)
    rmw = rclpy.get_rmw_implementation_identifier()
    generation = uuid.uuid4().hex
    process_start_ticks = int(Path('/proc/self/stat').read_text().rsplit(')', 1)[1].split()[19])
    name = f'r2f_{role}_{generation[:12]}'
    node = Node(name, context=context, enable_rosout=False, start_parameter_services=False)
    executor = SingleThreadedExecutor(context=context)
    executor.add_node(node)
    receive_sequence = 0
    local_gid = None

    def emit(event, **fields):
        value = {'event': event, 'time_ns': time.time_ns(), 'monotonic_ns': time.monotonic_ns(),
                 'role': role, 'pid': os.getpid(), 'pgid': os.getpgrp(), 'run_id': config['run_id'],
                 'domain_id': config['domain_id'], 'rmw_identifier': rmw,
                 'endpoint_generation': generation, 'node_name': name, 'process_start_ticks': process_start_ticks}
        value.update(fields)
        print(json.dumps(value, sort_keys=True), flush=True)
        return value

    # Recreated reader/writer generations use explicit role aliases but the
    # same endpoint semantics and QoS contract as their base role.  Keep this
    # mapping in one place so reader2..reader9 and writer2..writer4 cannot
    # drift.  projection_pair supplies the exact per-role profile; direct
    # worker invocations fall back to the base kind.
    import re
    match = re.fullmatch(r'(reader|writer)([2-9])', role)
    endpoint_role = match.group(1) if match else role
    q = config.get(role + '_qos', config.get(endpoint_role + '_qos', config['qos']))
    qos = create_public_qos(q)

    def gid(value):
        return bytes(value).hex() if value is not None else None

    def receive(message, info):
        nonlocal receive_sequence
        receive_sequence += 1
        try:
            body = json.loads(message.data)
        except (ValueError, TypeError):
            emit('receive_invalid_payload', receive_sequence=receive_sequence)
            return
        get_info = info.get if isinstance(info, dict) else lambda key: getattr(info, key, None)
        metadata = {}
        for key in ('source_timestamp', 'received_timestamp', 'publication_sequence_number', 'reception_sequence_number'):
            value = get_info(key)
            metadata[key] = value if isinstance(value, (int, str, bool, type(None))) else str(value)
        emit('receive', send_sequence=body.get('send_sequence'), receive_sequence=receive_sequence,
             phase=body.get('phase'), payload_run_id=body.get('run_id'),
             publisher_gid=gid(get_info('publisher_gid')), reader_gid=local_gid,
             message_info_type=type(info).__name__,
             message_info_fields=sorted(info) if isinstance(info, dict) else [],
             publisher_gid_status='available' if get_info('publisher_gid') is not None else 'unavailable_api',
             identity_basis='message_info_gid' if get_info('publisher_gid') is not None else 'endpoint_correlated',
             message_info=metadata)

    create_begin = emit('endpoint_create_begin', topic=config['topic'], qos=q, message_type='std_msgs/msg/String')
    if endpoint_role == 'probe':
        # Independent participant/endpoint used only to generate real discovery
        # announcements during an interruption window.  It never joins the
        # writer/reader pair and its topic is deliberately distinct.
        probe_topic = config.get('probe_topic', '/r2f/probe/discovery_only')
        endpoint = node.create_publisher(String, probe_topic, qos)
    elif endpoint_role == 'writer':
        endpoint = node.create_publisher(String, config['topic'], qos)
    else:
        endpoint = node.create_subscription(String, config['topic'], receive, qos)
    created_ns = time.monotonic_ns()

    def graph():
        nonlocal local_gid
        def entry(info):
            return {'node_name': info.node_name, 'node_namespace': info.node_namespace,
                    'topic_type': info.topic_type, 'endpoint_gid': gid(info.endpoint_gid),
                    'qos': {'reliability': info.qos_profile.reliability.name.lower(),
                            'durability': info.qos_profile.durability.name.lower(),
                            'depth': info.qos_profile.depth,
                            **extended_graph_qos(info.qos_profile, config.get('observe_extended_qos', False))}}
        graph_topic = (config.get('probe_topic', '/r2f/probe/discovery_only')
                       if endpoint_role == 'probe' else config['topic'])
        publishers = [entry(x) for x in node.get_publishers_info_by_topic(graph_topic)]
        subscriptions = [entry(x) for x in node.get_subscriptions_info_by_topic(graph_topic)]
        for item in publishers if endpoint_role in ('writer', 'probe') else subscriptions:
            if item['node_name'] == name:
                local_gid = item['endpoint_gid']
        return {'publishers': publishers, 'subscriptions': subscriptions}

    initial_graph = graph()
    identity_deadline = time.monotonic() + config['startup_timeout_sec']
    while local_gid is None and time.monotonic() < identity_deadline:
        executor.spin_once(timeout_sec=0.02)
        initial_graph = graph()
    emit('endpoint_created', endpoint_gid=local_gid, created_monotonic_ns=created_ns,
         create_begin_monotonic_ns=create_begin['monotonic_ns'], create_begin_time_ns=create_begin['time_ns'],
         topic=config['topic'], qos=q, message_type='std_msgs/msg/String')
    emit('graph_snapshot', **initial_graph)
    if str(config.get('target_id', '')).endswith('.fuzz_v1'):
        # The only observed values used for exploratory parameter binding come
        # from the same SDK graph call that reports this endpoint's GID.
        own = self_qos_entry(initial_graph, role, local_gid) if local_gid else None
        emit('qos_binding_observed', requested=q,
             observed_self=own['qos'] if own else None,
             observed_self_gid=own['endpoint_gid'] if own else None,
             observation_api='get_%s_info_by_topic' % ('publishers' if endpoint_role in ('writer', 'probe') else 'subscriptions'))
    input_buffer = b''
    pending_commands = []
    last_count = None
    last_graph_ns = 0
    try:
        while True:
            executor.spin_once(timeout_sec=0.02)
            now = time.monotonic_ns()
            count = (endpoint.get_subscription_count()
                     if endpoint_role in ('writer', 'probe') else endpoint.get_publisher_count())
            if count != last_count:
                emit('match', matched_count=count, endpoint_gid=local_gid)
                emit('graph_snapshot', **graph())
                last_count = count
            if now - last_graph_ns > 500_000_000:
                emit('graph_snapshot', **graph())
                last_graph_ns = now
            if select.select([sys.stdin], [], [], 0)[0]:
                chunk = os.read(sys.stdin.fileno(), 65536)
                if not chunk:
                    break
                input_buffer += chunk
                while b'\n' in input_buffer:
                    line, input_buffer = input_buffer.split(b'\n', 1)
                    pending_commands.append(line)
            if not pending_commands:
                continue
            command = json.loads(pending_commands.pop(0))
            if command['command'] == 'stop':
                emit('worker_stopping')
                break
            if command['command'] == 'snapshot':
                emit_snapshot(endpoint, role, command['command_id'], local_gid, graph, emit)
                emit('command_done', command_id=command['command_id'])
            elif command['command'] == 'publish' and endpoint_role == 'writer':
                sequence = command['send_sequence']
                phase = command['phase']
                if command.get('omit', False):
                    emit('harness_omission', send_sequence=sequence, phase=phase,
                         fault_id=config.get('fault_id') or 'late_endpoint_delivery_omission', responsibility_layer='harness',
                         candidate_eligible=False, omission_layer='before_public_publish_call')
                else:
                    payload = {'run_id': config['run_id'], 'send_sequence': sequence, 'phase': phase}
                    message = String()
                    message.data = json.dumps(payload, sort_keys=True)
                    endpoint.publish(message)
                    emit('send', send_sequence=sequence, phase=phase, writer_gid=local_gid,
                         publish_returned=True, payload_sha256=__import__('hashlib').sha256(message.data.encode()).hexdigest())
                emit('command_done', command_id=command['command_id'])
            elif command['command'] == 'publish_burst' and endpoint_role == 'writer':
                # Publish a bounded sequence without per-sample controller ACKs;
                # one command_done closes the burst.  This creates real
                # publish/creation overlap rather than a serialized schedule.
                sequences = command.get('sequences')
                phase = command.get('phase', 'post_restore')
                if (not isinstance(sequences, list) or not sequences
                        or any(type(x) is not int for x in sequences)):
                    emit('worker_command_error', command_id=command['command_id'],
                         error='invalid_burst_sequences')
                else:
                    interval = command.get('interval_sec', 0.0)
                    if type(interval) not in (int, float) or interval < 0 or interval > 1.0:
                        emit('worker_command_error', command_id=command['command_id'],
                             error='invalid_burst_interval')
                    else:
                        for index, sequence in enumerate(sequences):
                            payload = {'run_id': config['run_id'], 'send_sequence': sequence, 'phase': phase}
                            message = String()
                            message.data = json.dumps(payload, sort_keys=True)
                            endpoint.publish(message)
                            emit('send', send_sequence=sequence, phase=phase, writer_gid=local_gid,
                                 publish_returned=True, burst=True,
                                 payload_sha256=__import__('hashlib').sha256(message.data.encode()).hexdigest())
                            if interval and index + 1 < len(sequences):
                                end = time.monotonic() + float(interval)
                                while time.monotonic() < end:
                                    executor.spin_once(timeout_sec=min(0.01, max(0.0, end - time.monotonic())))
                        emit('command_done', command_id=command['command_id'], burst_count=len(sequences))
    finally:
        executor.shutdown()
        node.destroy_node()
        context.shutdown()
        emit('worker_shutdown')


if __name__ == '__main__':
    try:
        main()
    except BaseException as exc:
        print(json.dumps({'event': 'worker_error', 'pid': os.getpid(), 'pgid': os.getpgrp(),
                          'time_ns': time.time_ns(), 'monotonic_ns': time.monotonic_ns(),
                          'error': type(exc).__name__ + ':' + str(exc),
                          'traceback': traceback.format_exc()}), flush=True)
        sys.exit(1)
