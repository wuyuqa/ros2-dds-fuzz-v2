"""Non-DDS namespace builder and fixed-probe contracts; no ROS imports/launches."""
import copy
import json
from pathlib import Path
from unittest.mock import patch

import pytest

from r2f.runtime import discovery_network as network
from r2f.runtime import execution_boundary as boundary

HOST = {'host_uid': 1000, 'host_cap_eff': 0, 'host_user_namespace': 'user:outer',
        'host_net_namespace': 'net:outer', 'launcher_pid': 1}


def test_default_namespace_command_is_byte_exact(monkeypatch):
    monkeypatch.delenv('R2F_DISCOVERY_NETNS_PROOF', raising=False)
    with patch.object(network, '_host_identity', return_value=HOST):
        expected = ['unshare', '--user', '--map-root-user', '--net', '--', '/python', '-B',
                    '-m', network.__name__, '_enter', json.dumps(HOST, sort_keys=True), '--', 'controller']
        assert network.namespace_command(['controller'], python_executable='/python') == expected
        assert network.namespace_command(['controller'], python_executable='/python', pid_mount=False) == expected


def test_pid_mount_is_explicit_and_keeps_kernel_wrapper(monkeypatch):
    monkeypatch.delenv('R2F_DISCOVERY_NETNS_PROOF', raising=False)
    with patch.object(network, '_host_identity', return_value=HOST):
        argv = network.namespace_command(['controller'], python_executable='/python', pid_mount=True)
    assert argv[:11] == ['unshare', '--user', '--map-root-user', '--net', '--pid', '--fork',
                         '--mount', '--mount-proc', '--kill-child=SIGKILL', '--', '/python']
    assert argv[-2:] == ['--', 'controller']
    assert '_enter' in argv


@pytest.mark.parametrize('value', [None, 1, 'true', {}, []])
def test_pid_mount_rejects_flags_as_truth(value):
    with pytest.raises(ValueError, match='pid_mount must be bool'):
        network.namespace_command(['controller'], pid_mount=value)


def test_pid_mount_retains_existing_proof_denial(monkeypatch):
    monkeypatch.setenv('R2F_DISCOVERY_NETNS_PROOF', '{}')
    with patch.object(network, '_host_identity', return_value=HOST), pytest.raises(network.NetworkIsolationError):
        network.namespace_command(['controller'], pid_mount=True)


def test_fixed_probe_mode_has_no_callback_or_formal_credit(tmp_path):
    with patch.object(boundary, '_source', return_value={'source_frozen': False}), patch.object(
            boundary, '_supervise', return_value={'fixture': True}) as supervise:
        assert boundary.probe_execution_boundary(tmp_path, mode='nested_pid_proc') == {'fixture': True}
    assert supervise.call_args.kwargs['probe'] == {'mode': 'nested_pid_proc', 'preflight_rmws': []}
    assert supervise.call_args.args[3]['source_frozen_required'] is False
    with pytest.raises(ValueError):
        boundary.probe_execution_boundary(tmp_path, mode='callback')
    with pytest.raises(boundary.BoundaryError):
        boundary._options({'enabled': True, 'nested_pid_proc': True})


def scope_fixture(monkeypatch, pid=1, same_namespace=None, mount_text=None):
    outer = {'namespaces': {name: name+':outer' for name in boundary.NAMESPACES}}
    actual = {'namespaces': {name: name+':inner' for name in boundary.NAMESPACES},
              'start_ticks': 19, 'nspid': [pid]}
    if same_namespace:
        actual['namespaces'][same_namespace] = outer['namespaces'][same_namespace]
    monkeypatch.setattr(boundary, '_identity', lambda: actual)
    monkeypatch.setattr(boundary.os, 'getpid', lambda: pid)
    monkeypatch.setattr(boundary, '_ticks', lambda p: 19)
    monkeypatch.setattr(boundary, '_active_mount_id', lambda p: '12')
    monkeypatch.setattr(boundary.os, 'readlink', lambda p: actual['namespaces']['pid'])
    monkeypatch.setattr(Path, 'read_text', lambda p: mount_text or '12 11 0:1 / /proc rw - proc proc rw\n')
    monkeypatch.setattr(network, 'assert_isolated_namespace', lambda: {'isolated': True})
    return outer


def test_fixed_scope_requires_fresh_pid_mount_and_proc(monkeypatch):
    outer = scope_fixture(monkeypatch)
    assert boundary._nested_proc_scope(outer)['pid1_proc_scoped'] is True


@pytest.mark.parametrize('pid,same,mount,error', [
    (2, None, None, 'not_pid1'), (1, 'pid', None, 'not_independent:pid'),
    (1, 'mnt', None, 'not_independent:mnt'), (1, None, '12 11 0:1 / /proc rw - tmpfs tmpfs rw', 'mount_missing'),
])
def test_fixed_scope_rejects_bad_kernel_scope(monkeypatch, pid, same, mount, error):
    outer = scope_fixture(monkeypatch, pid, same, mount)
    with pytest.raises(boundary.BoundaryError, match=error):
        boundary._nested_proc_scope(outer)


def test_fixed_scope_missing_discovery_proof_cannot_qualify(monkeypatch):
    outer = scope_fixture(monkeypatch)
    def missing():
        raise network.NetworkIsolationError('missing real proof')
    monkeypatch.setattr(network, 'assert_isolated_namespace', missing)
    with pytest.raises(network.NetworkIsolationError):
        boundary._nested_proc_scope(outer)


def test_probe_source_observation_is_not_freeze_bypass(monkeypatch):
    observed = {'head': 'h', 'source_repo': '/r', 'behavior_tree_sha256': 's', 'source_frozen': False}
    monkeypatch.setattr(boundary, '_source', lambda: observed)
    assert boundary._fixed_probe_source(observed)['source_frozen'] is False
    with pytest.raises(boundary.BoundaryError, match='changed'):
        boundary._fixed_probe_source(dict(observed, behavior_tree_sha256='other'))
    with pytest.raises(boundary.BoundaryError, match='not_frozen'):
        boundary._same_source(dict(observed, source_frozen=True))


@pytest.mark.parametrize('packet,fds', [({}, []), ({'kind': 'nested_fixed_ready'}, [1, 2])])
def test_fixed_parent_rejects_missing_kernel_descriptors(packet, fds):
    with pytest.raises(boundary.BoundaryError, match='fd_count_or_kind'):
        boundary._bind_nested_probe(packet, (1, 1000, 1000), fds, {})


def test_scope_selects_active_proc_mount_not_hidden_stack(monkeypatch):
    outer = scope_fixture(monkeypatch, mount_text=(
        '10 9 0:1 / /proc rw - proc proc rw\n'
        '12 11 0:2 / /proc rw - proc proc rw\n'))
    assert boundary._nested_proc_scope(outer)['active_proc_mount_id'] == '12'
    monkeypatch.setattr(boundary, '_active_mount_id', lambda p: '99')
    with pytest.raises(boundary.BoundaryError, match='mount_missing'):
        boundary._nested_proc_scope(outer)


def test_scope_rejects_visible_nonproc_even_with_hidden_proc(monkeypatch):
    outer = scope_fixture(monkeypatch, mount_text=(
        '10 9 0:1 / /proc rw - proc proc rw\n'
        '12 11 0:2 / /proc rw - tmpfs tmpfs rw\n'))
    with pytest.raises(boundary.BoundaryError, match='mount_missing'):
        boundary._nested_proc_scope(outer)

# Production bridge contracts below use explicit kernel/process doubles. None of
# these fixtures is an execution proof or starts a namespace/ROS process.
from types import SimpleNamespace
from unittest.mock import Mock, MagicMock
from r2f.runtime import topology_namespace as bridge
from r2f.runtime.topology_worker import TopologyBackendBlocked, ROS_BACKEND


def identity(pid, scope, *, uid=0):
    return {'pid': pid, 'nspid': [pid], 'start_ticks': 123,
            'uid': uid, 'cap_eff': 1 << 12 if uid == 0 else 0,
            'uid_map': '0 1000 1\n' if uid == 0 else '1000 1000 1\n',
            'gid_map': '0 1000 1\n' if uid == 0 else '1000 1000 1\n',
            'namespaces': {n: n + ':[' + scope + ']' for n in boundary.NAMESPACES}}


def outer_request():
    cfg = {'target_id': 'discovery.endpoint_topology', 'execution_boundary': {'enabled': True},
           'backend': ROS_BACKEND, 'lock_root': '/OUT/inner-locks'}
    # Obtain the actual target constant rather than rely on a prose profile.
    from r2f.runtime.topology_route import TOPOLOGY_TARGET
    cfg['target_id'] = TOPOLOGY_TARGET
    return cfg, {'config': copy.deepcopy(cfg), 'inner_config_sha256': boundary._hash(cfg),
                 'host': identity(99, 'host', uid=1000),
                 'identity': {'source_manifest': {'source_frozen': True}},
                 'options': {'startup_timeout_sec': 1, 'timeout_sec': 1,
                             'kill_wait_sec': .1, 'term_grace_sec': .1}}


@pytest.mark.parametrize('backend,enabled,wanted', [
    (None, True, True), (ROS_BACKEND, True, True), ('offline_fake', True, False),
    ('injected', True, False), (ROS_BACKEND, False, False), (ROS_BACKEND, 1, False),
])
def test_production_route_is_request_not_proof(backend, enabled, wanted):
    cfg, _ = outer_request()
    cfg.update(backend=backend, execution_boundary={'enabled': enabled})
    original = copy.deepcopy(cfg)
    assert bridge.route_requested(cfg) is wanted
    assert cfg == original and 'live_execution_go' not in cfg


def test_profile_prose_or_environment_never_routes(monkeypatch):
    monkeypatch.setenv('R2F_DISCOVERY_NETNS_PROOF', '{"isolated":true}')
    cfg = {'profile': 'isolated topology', 'execution_boundary': {'enabled': True}}
    assert bridge.route_requested(cfg) is False
    monkeypatch.setattr(bridge, '_ACTIVE_CHILD', {'authenticated': True})
    assert bridge.current_child_context() is None


def gate_fixture(monkeypatch):
    cfg, req = outer_request()
    actual = identity(1, 'outer', uid=1000)
    monkeypatch.setattr(boundary, '_identity', lambda: actual)
    monkeypatch.setattr(boundary.os, 'getpid', lambda: 1)
    monkeypatch.setattr(boundary, '_same_source', Mock(return_value=req['identity']['source_manifest']))
    monkeypatch.setattr('r2f.runtime.topology_route.validate_topology_route', Mock())
    return cfg, req, actual


def test_outer_gate_uses_actual_kernel_and_unchanged_config(monkeypatch):
    cfg, req, actual = gate_fixture(monkeypatch)
    assert bridge._outer_gate(cfg, req) == actual
    boundary._same_source.assert_called_once_with(req['identity']['source_manifest'])
    assert 'live_execution_go' not in cfg


@pytest.mark.parametrize('change,reason', [
    ('uid', 'uid1000_cap0_pid1'), ('caps', 'uid1000_cap0_pid1'),
    ('pid', 'uid1000_cap0_pid1'), ('uid_map', 'uid1000_cap0_pid1'),
    ('same_net', 'not_independent:net'), ('config', 'canonical_boundary_request'),
    ('hash', 'config_hash_mismatch'), ('source', 'source drift'),
])
def test_outer_gate_rejects_without_launch(monkeypatch, change, reason):
    cfg, req, actual = gate_fixture(monkeypatch)
    if change == 'uid': actual['uid'] = 0
    elif change == 'caps': actual['cap_eff'] = 1 << 12
    elif change == 'pid': monkeypatch.setattr(boundary.os, 'getpid', lambda: 2)
    elif change == 'uid_map': actual['uid_map'] = '0 1000 1\n'
    elif change == 'same_net': actual['namespaces']['net'] = req['host']['namespaces']['net']
    elif change == 'config': cfg['live_execution_go'] = True
    elif change == 'hash': req['inner_config_sha256'] = '0' * 64
    elif change == 'source': boundary._same_source.side_effect = boundary.BoundaryError('source drift')
    with pytest.raises(boundary.BoundaryError, match=reason):
        bridge._outer_gate(cfg, req)


def binding_fixture(monkeypatch):
    outer = identity(1, 'outer', uid=1000)
    reported = identity(1, 'nested')
    actual = copy.deepcopy(reported)
    actual.update(pid=3, nspid=[3, 1], uid=1000)
    packet = {'kind': 'ready', 'proof': {'identity': reported}}
    monkeypatch.setattr(boundary, '_identity', lambda p: actual)
    monkeypatch.setattr(boundary, '_pidfd_pid', lambda fd: 3)
    monkeypatch.setattr(boundary, '_parent_of', lambda p: 2)
    monkeypatch.setattr(boundary, '_exited', lambda fd: False)
    monkeypatch.setattr(boundary, '_ticks', lambda p: 123)
    monkeypatch.setattr(boundary.os, 'getgid', lambda: 1000)
    stat = SimpleNamespace(st_dev=7, st_ino=8)
    monkeypatch.setattr(bridge.os, 'stat', lambda *a, **kw: stat)
    monkeypatch.setattr(bridge.os, 'fstat', lambda *a, **kw: stat)
    monkeypatch.setattr(boundary.fcntl, 'ioctl', lambda fd, cmd: boundary.CLONE_FLAGS[boundary.NAMESPACES[fd-11]])
    return packet, (3, 1000, 1000), [10, 11, 12, 13, 14], outer, actual


def test_parent_authenticates_namespace_chain_not_numeric_pid1(monkeypatch):
    packet, peer, fds, outer, actual = binding_fixture(monkeypatch)
    result = bridge._bind_controller(packet, peer, fds, outer, 2)
    assert set(result) == {'authentication', 'parent_pid_scope', 'outer', 'reported_inner',
                           'inner_pid', 'pid_scope', 'pid_namespace_chain'}
    for name in ('outer', 'reported_inner'):
        assert set(result[name]) == {'pid', 'start_ticks', 'namespaces', 'uid', 'cap_eff',
                                     'nspid', 'uid_map', 'gid_map'}
    assert result['outer'] == actual
    assert result['reported_inner']['pid'] == 1
    assert result['pid_namespace_chain'] == [
        {'pid_scope': outer['namespaces']['pid'], 'pid': 3},
        {'pid_scope': actual['namespaces']['pid'], 'pid': 1}]
    assert result['outer']['uid_map'] == '0 1000 1\n'


@pytest.mark.parametrize('change,reason', [
    ('count', 'fd_count'), ('credential', 'credentials_pidfd_parent'),
    ('parent', 'credentials_pidfd_parent'), ('exited', 'credentials_pidfd_parent'),
    ('ticks', 'kernel_identity'), ('map', 'kernel_identity'),
    ('gid_map', 'kernel_identity'), ('nspid', 'kernel_identity'),
    ('reported_uid', 'kernel_identity'), ('caps', 'kernel_identity'),
    ('ns_type', 'nsfd_mismatch'), ('same_namespace', 'nsfd_mismatch'),
])
def test_parent_rejects_unbound_or_forged_controller(monkeypatch, change, reason):
    packet, peer, fds, outer, actual = binding_fixture(monkeypatch)
    if change == 'count': fds.pop()
    elif change == 'credential': peer = (4, 1000, 1000)
    elif change == 'parent': monkeypatch.setattr(boundary, '_parent_of', lambda p: 99)
    elif change == 'exited': monkeypatch.setattr(boundary, '_exited', lambda fd: True)
    elif change == 'ticks': monkeypatch.setattr(boundary, '_ticks', lambda p: 124)
    elif change == 'map': actual['uid_map'] = '0 0 1\n'
    elif change == 'gid_map': actual['gid_map'] = '0 999 1\n'
    elif change == 'nspid': actual['nspid'] = [3, 3]
    elif change == 'reported_uid': packet['proof']['identity']['uid'] = 1000
    elif change == 'caps': actual['cap_eff'] = 0
    elif change == 'ns_type': monkeypatch.setattr(boundary.fcntl, 'ioctl', lambda *a: 0)
    elif change == 'same_namespace': outer['namespaces']['net'] = actual['namespaces']['net']
    with pytest.raises(boundary.BoundaryError, match=reason):
        bridge._bind_controller(packet, peer, fds, outer, 2)


def context_fixture(tmp_path, monkeypatch):
    cfg, _ = outer_request()
    cfg['lock_root'] = str(tmp_path / 'locks')
    proof = {'identity': identity(1, 'nested')}
    req = {'outer': identity(1, 'outer', uid=1000), 'config': cfg,
           'source_manifest': {'source_frozen': True}, 'config_sha256': boundary._hash(cfg)}
    monkeypatch.setattr(boundary, '_nested_proc_scope', lambda p: copy.deepcopy(proof))
    monkeypatch.setattr(boundary, '_same_source', Mock(return_value=req['source_manifest']))
    receipts = [{'path': str(tmp_path / name), 'acquired': True, 'owner': {'pid': 1}}
                for name in ('campaign', 'domain', 'state')]
    verify = Mock(return_value=receipts)
    monkeypatch.setattr('r2f.runtime.ros2_pair.verify_pair_lease', verify)
    events = []
    def emit(event, **fields):
        row = copy.deepcopy({'event': event, 'event_index': len(events), **fields})
        events.append(row)
        return row
    return bridge._ChildContext(req, proof), cfg, events, emit, verify


def test_nested_leases_are_canonical_receipts_not_flags(tmp_path, monkeypatch):
    ctx, cfg, events, emit, verify = context_fixture(tmp_path, monkeypatch)
    lease = object()
    ctx.leases_ready(lease, cfg, tmp_path, emit)
    verify.assert_called_once_with(lease, cfg, tmp_path)
    assert set(ctx.lease_scope) == {'pid_scope', 'controller_start_ticks', 'owner_pid',
                                    'lock_root', 'state_dir', 'receipts', 'raw_event_ref'}
    assert ctx.lease_scope['owner_pid'] == 1
    assert ctx.lease_scope['pid_scope'] == 'pid:[nested]'
    assert ctx.lease_scope['raw_event_ref'] == 0
    assert 'raw_event_ref' not in events[0]['lease_scope']  # no alias/backfill


@pytest.mark.parametrize('change', ['lease', 'scope', 'root', 'source'])
def test_nested_lease_failures_remain_blocked(tmp_path, monkeypatch, change):
    ctx, cfg, events, emit, verify = context_fixture(tmp_path, monkeypatch)
    if change == 'lease': verify.side_effect = RuntimeError('not lease owner')
    elif change == 'scope': monkeypatch.setattr(boundary, '_nested_proc_scope', lambda p: {'identity': identity(2, 'foreign')})
    elif change == 'root': cfg = dict(cfg, lock_root=str(tmp_path / 'foreign'))
    elif change == 'source': boundary._same_source.side_effect = RuntimeError('source changed')
    with pytest.raises(TopologyBackendBlocked, match='lease_admission'):
        ctx.leases_ready(object(), cfg, tmp_path, emit)
    assert ctx.lease_scope is None and events == []


def worker_fixture(tmp_path, monkeypatch):
    ctx, cfg, events, emit, verify = context_fixture(tmp_path, monkeypatch)
    ctx.leases_ready(object(), cfg, tmp_path, emit)
    handle = dict(logical_id='w', generation=1, endpoint_gid='ab' * 16,
                  endpoint_generation='gen', pid=9, process_start_ticks=123,
                  process=SimpleNamespace(process=SimpleNamespace(pid=9, poll=lambda: None)))
    monkeypatch.setattr(boundary, '_identity', lambda pid: identity(pid, 'nested'))
    monkeypatch.setattr(boundary, '_ticks', lambda pid: 123)
    return ctx, handle, events, emit


def test_worker_receipt_binds_actual_process_and_generation(tmp_path, monkeypatch):
    ctx, handle, events, emit = worker_fixture(tmp_path, monkeypatch)
    ctx.worker_created(handle, emit)
    binding = ctx.worker_bindings[0]
    assert set(binding) == {'logical_id', 'generation', 'endpoint_gid', 'endpoint_generation',
                            'pid', 'process_start_ticks', 'namespaces', 'outer_pid',
                            'host_pid', 'mapping_status', 'raw_event_ref'}
    assert binding['raw_event_ref'] == 1
    assert binding['outer_pid'] is None and binding['host_pid'] is None
    assert 'raw_event_ref' not in events[1]['binding']


@pytest.mark.parametrize('change', ['no_leases', 'exited', 'wrong_pid', 'ticks', 'namespace', 'gid'])
def test_worker_identity_mismatch_never_becomes_receipt(tmp_path, monkeypatch, change):
    ctx, handle, events, emit = worker_fixture(tmp_path, monkeypatch)
    if change == 'no_leases': ctx.lease_scope = None
    elif change == 'exited': handle['process'].process.poll = lambda: 0
    elif change == 'wrong_pid': handle['pid'] = 10
    elif change == 'ticks': handle['process_start_ticks'] = 124
    elif change == 'namespace': monkeypatch.setattr(boundary, '_identity', lambda pid: identity(pid, 'foreign'))
    elif change == 'gid': handle['endpoint_gid'] = None
    with pytest.raises(TopologyBackendBlocked, match='worker_admission'):
        ctx.worker_created(handle, emit)
    assert ctx.worker_bindings == [] and len(events) == 1


def artifact_fixture(tmp_path, monkeypatch):
    ctx, handle, events, emit = worker_fixture(tmp_path, monkeypatch)
    ctx.worker_created(handle, emit)
    raw = tmp_path / 'raw.jsonl'
    raw.write_text(''.join(json.dumps(e, sort_keys=True) + '\n' for e in events))
    worker = {k: handle[k] for k in bridge._WORKER_KEYS}
    result = {'raw_path': str(raw), 'raw_sha256': bridge._sha(raw), 'events': events,
              'real_execution': True, 'execution_metadata': {'workers': [dict(worker, real_ros=True)]}}
    envelope = {'inner_lease_scope': ctx.lease_scope, 'worker_bindings': ctx.worker_bindings,
                'raw_ref': bridge._ref(raw, 'jsonl')}
    return result, envelope


def test_immutable_raw_refs_bind_receipts_without_rewriting(tmp_path, monkeypatch):
    runtime, envelope = artifact_fixture(tmp_path, monkeypatch)
    before = Path(runtime['raw_path']).read_bytes(), copy.deepcopy(runtime), copy.deepcopy(envelope)
    leases, workers = bridge._receipt_refs(envelope, runtime, tmp_path, 'pid:[nested]')
    for ref in [leases['raw_event_ref'], workers[0]['raw_event_ref']]:
        assert ref['path'] == runtime['raw_path'] and ref['sha256'] == runtime['raw_sha256']
        assert type(ref['event_index']) is int
    assert before == (Path(runtime['raw_path']).read_bytes(), runtime, envelope)


@pytest.mark.parametrize('change,reason', [
    ('hash', 'path_or_hash'), ('event', 'raw_result'), ('scope', 'lease_scope'),
    ('owner', 'lease_scope'), ('receipts', 'lease_scope'), ('offset', 'raw_ref_invalid'),
    ('worker_bytes', 'worker_raw_bytes'), ('metadata', 'generation_unbound'),
    ('no_worker', 'missing_actual_worker'), ('no_scope', 'missing_actual_worker'),
])
def test_raw_receipt_and_worker_metadata_must_all_agree(tmp_path, monkeypatch, change, reason):
    runtime, env = artifact_fixture(tmp_path, monkeypatch)
    if change == 'hash': env['raw_ref']['sha256'] = '0' * 64
    elif change == 'event': runtime['events'] = []
    elif change == 'scope': env['inner_lease_scope']['pid_scope'] = 'pid:[outer]'
    elif change == 'owner': env['inner_lease_scope']['owner_pid'] = 2
    elif change == 'receipts': env['inner_lease_scope']['receipts'] = []
    elif change == 'offset': env['worker_bindings'][0]['raw_event_ref'] = True
    elif change == 'worker_bytes': env['worker_bindings'][0]['generation'] = 99
    elif change == 'metadata': runtime['execution_metadata']['workers'][0]['generation'] = 99
    elif change == 'no_worker': env['worker_bindings'] = []
    elif change == 'no_scope': env['inner_lease_scope'] = None
    with pytest.raises(boundary.BoundaryError, match=reason):
        bridge._receipt_refs(env, runtime, tmp_path, 'pid:[nested]')


@pytest.mark.parametrize('change', ['outside', 'symlink', 'kind', 'hash'])
def test_artifact_refs_stay_within_output_and_hash_bound(tmp_path, change):
    root = tmp_path / 'root'
    root.mkdir()
    inside, outside = root / 'data.json', tmp_path / 'other.json'
    inside.write_text('{}')
    outside.write_text('{}')
    ref = bridge._ref(inside)
    if change == 'outside': ref = bridge._ref(outside)
    elif change == 'symlink':
        inside.unlink()
        inside.symlink_to(outside)
    elif change == 'kind': ref['kind'] = 'jsonl'
    elif change == 'hash': ref['sha256'] = '0' * 64
    with pytest.raises(boundary.BoundaryError, match='path_or_hash'):
        bridge._load_ref(ref, root)


def test_legacy_identity_annotation_is_exact_noop():
    ctx, raw = {'controller_binding': {'pid_scope': 'outer'}}, {'pid_namespace': 'outer'}
    before = copy.deepcopy((ctx, raw))
    boundary._annotate_nested_identity({}, ctx, raw)
    assert before == (ctx, raw)


def test_new_annotation_keeps_outer_binding_and_actual_raw_scope():
    ctx, raw = {'controller_binding': {'pid_scope': 'outer'}}, {'pid_namespace': 'outer'}
    descriptor = {'nested_controller_binding': {'pid_scope': 'nested'}, 'raw_pid_scope': 'nested'}
    boundary._annotate_nested_identity({'topology_namespace': descriptor}, ctx, raw)
    assert ctx['controller_binding'] == {'pid_scope': 'outer'}
    assert ctx['raw_pid_scope'] == raw['pid_namespace'] == 'nested'


def formal_topology_config():
    from r2f.core import Registry
    from r2f.core.replay import ReplayCapsule
    from r2f.core.scenario import Scenario
    from r2f.modules.s1.module import register_exploratory
    from r2f.modules.s1 import topology_model as model
    from r2f.modules.s1.topology_input import make_topology_plan
    from r2f.runtime.topology_route import TOPOLOGY_TARGET, TOPOLOGY_CAPABILITY
    registry = Registry()
    register_exploratory(registry, runtime_capability=TOPOLOGY_CAPABILITY)
    target = registry.get_target(TOPOLOGY_TARGET)
    plan = make_topology_plan(model.generate_program(0, writers=1, readers=1),
                              [{'operator_id': 'action.insert'}], rng_seed=7)
    scenario = Scenario(target, 'rmw_cyclonedds_cpp', 'mutated', 0, 7, {}, plan)
    prepared = registry.get_services(TOPOLOGY_TARGET).mutation_provider.prepare(scenario)
    source = {'source_frozen': True, 'source_repo': str(boundary.REPO), 'head': 'a' * 40,
              'behavior_tree_sha256': 'b' * 64, 'dirty_behavior_paths': []}
    capsule = ReplayCapsule.from_scenario(scenario, source_manifest=source,
        final_input=prepared['final_input'], input_layer_receipts=prepared['input_layer_receipts'])
    effective = capsule.input['effective_input']
    cfg = dict(copy.deepcopy(effective), effective_input=copy.deepcopy(effective),
               effective_input_sha256=capsule.input['effective_input_sha256'],
               plan_hash=plan.plan_hash(), scenario=scenario.to_dict(),
               runtime_capability=TOPOLOGY_CAPABILITY,
               topology_program=copy.deepcopy(plan.final_config['topology_program']),
               run_id='offline-bridge-contract', domain_id=191, backend=ROS_BACKEND,
               source_manifest=source, source_repo=str(boundary.REPO), expected_head=source['head'],
               expected_target_revision=0, execution_boundary={'enabled': True})
    return cfg


def test_canonical_topology_passes_unchanged_formal_identity_gate():
    config = formal_topology_config()
    original = copy.deepcopy(config)
    with patch.object(boundary, '_source', return_value=config['source_manifest']):
        result = boundary._formal_identity(config)
    assert result['config_sha256'] == boundary._hash(config)
    assert result['effective_input_sha256'] == config['effective_input_sha256']
    assert bridge.route_requested(config)
    assert config == original and 'live_execution_go' not in config


def test_no_go_still_blocks_real_backend_before_lease_or_namespace(tmp_path):
    from r2f.runtime.ros2_pair import run_pair
    config = formal_topology_config()
    with patch('r2f.runtime.ros2_pair.acquire_pair_environment', side_effect=AssertionError('NO LEASE')), patch.object(
            bridge.subprocess, 'Popen', side_effect=AssertionError('NO PROCESS')):
        result = run_pair(config, tmp_path)
    assert result['status'] == 'blocked' and result['reason'] == 'blocked:no_live_ros_go'
    assert not list(tmp_path.iterdir())


def parent_simulation(tmp_path, monkeypatch, failure=None):
    """Exercise real relay logic around explicitly simulated kernel/process IO."""
    cfg, request = outer_request()
    source = request['identity']['source_manifest']
    outer, nested = identity(1, 'outer', uid=1000), identity(1, 'nested')
    visible = dict(copy.deepcopy(nested), pid=3, nspid=[3, 1], uid=1000)
    binding = {'outer': visible, 'reported_inner': nested, 'pid_scope': 'pid:[nested]',
               'parent_pid_scope': 'pid:[outer]', 'inner_pid': 1,
               'pid_namespace_chain': [{'pid_scope': 'pid:[outer]', 'pid': 3},
                                       {'pid_scope': 'pid:[nested]', 'pid': 1}]}
    monkeypatch.setattr(bridge, '_outer_gate', lambda *a: outer)
    monkeypatch.setattr(bridge, '_bind_controller', lambda *a: binding)
    monkeypatch.setattr(boundary, '_same_source', Mock(return_value=source))
    monkeypatch.setattr(boundary, '_observe_owned_tree', Mock())
    command = Mock(side_effect=lambda argv, **kw: ['fake-unshare', *argv])
    monkeypatch.setattr(bridge, 'namespace_command', command)
    monkeypatch.setattr(boundary, '_pair', lambda: (Mock(), Mock(fileno=lambda: 99)))
    process = Mock(pid=2, returncode=0)
    cleanup = Mock(return_value={'confirmed': failure != 'cleanup', 'controller_exited': True,
                                'wrapper_returncode': 0, 'method': bridge.CLEANUP_METHOD})
    monkeypatch.setattr(bridge, '_cleanup', cleanup)
    sends = []
    monkeypatch.setattr(boundary, '_send', lambda sock, packet: sends.append(packet))
    state = {}
    def popen(argv, **kwargs):
        runtime, envelope = artifact_fixture(tmp_path, monkeypatch)
        side = tmp_path / 'topology-namespace'
        boundary._write(side / 'child-runtime-result.json', runtime)
        for name in ('source-before', 'source-after'):
            boundary._write(side / (name + '.json'),
                            {'source_frozen': False} if failure == 'source' and name == 'source-after' else source)
        envelope.update(inner_result_ref=bridge._ref(side / 'child-runtime-result.json'),
                        source_before_ref=bridge._ref(side / 'source-before.json'),
                        source_after_ref=bridge._ref(side / 'source-after.json'))
        boundary._write(side / 'child-envelope.json', envelope)
        state['envelope_hash'] = bridge._sha(side / 'child-envelope.json')
        state['raw_before'] = (tmp_path / 'raw.jsonl').read_bytes()
        state['child_before'] = (side / 'child-runtime-result.json').read_bytes()
        state['popen_kwargs'] = kwargs
        if failure == 'returncode': process.returncode = 127
        if failure == 'wait_timeout': process.wait.side_effect = bridge.subprocess.TimeoutExpired(argv, .1)
        return process
    monkeypatch.setattr(bridge.subprocess, 'Popen', popen)
    responses = iter([0, 1])
    def recv(sock, timeout):
        if next(responses) == 0:
            return {'kind': 'ready', 'config_sha256': boundary._hash(cfg)}, (3, 1000, 1000), []
        if failure == 'timeout':
            # Exactly exhaust deadline without waiting, no real process involved.
            monkeypatch.setattr(bridge.time, 'monotonic', lambda: float('inf'))
            raise TimeoutError('simulated receive timeout')
        if failure == 'eof': raise EOFError('simulated child failure')
        return {'kind': 'result', 'envelope_sha256': ('0' * 64 if failure == 'hash' else state['envelope_hash'])}, (
            (4, 1000, 1000) if failure == 'credentials' else (3, 1000, 1000)), []
    monkeypatch.setattr(boundary, '_recv', recv)
    # context_fixture (called in simulated Popen) installs same_source double.
    result = bridge.run_isolated_topology(cfg, tmp_path, boundary_request=request)
    return result, state, cleanup, command, sends


def test_parent_relay_freezes_acyclic_manifest_and_original_result(tmp_path, monkeypatch):
    result, state, cleanup, command, sends = parent_simulation(tmp_path, monkeypatch)
    descriptor = result['topology_namespace']
    manifest_path = Path(descriptor['manifest_path'])
    assert descriptor['manifest_sha256'] == bridge._sha(manifest_path)
    manifest = json.loads(manifest_path.read_bytes())
    assert manifest['error'] is None and manifest['cleanup']['confirmed']
    assert 'manifest_sha256' not in manifest
    child_result = bridge._load_ref(manifest['inner_result_ref'], tmp_path)
    assert 'topology_namespace' not in child_result
    assert Path(manifest['inner_result_ref']['path']).read_bytes() == state['child_before']
    assert Path(manifest['raw_ref']['path']).read_bytes() == state['raw_before']
    assert 'live_execution_go' not in bridge._load_ref(manifest['request_ref'], tmp_path)['config']
    assert command.call_args.kwargs == {'pid_mount': True}
    assert state['popen_kwargs']['start_new_session'] is True
    assert state['popen_kwargs']['pass_fds'] == (99,)
    assert [s['kind'] for s in sends] == ['execute', 'result_ack']
    cleanup.assert_called_once()


@pytest.mark.parametrize('failure,reason', [
    ('source', 'source_artifact_mismatch'), ('hash', 'path_or_hash'),
    ('credentials', 'unbound_result'), ('eof', 'EOFError'),
    ('returncode', 'child_failed:127'), ('wait_timeout', 'TimeoutExpired'),
    ('cleanup', 'cleanup_unconfirmed'), ('timeout', 'runtime_timeout'),
])
def test_relay_failure_preserves_child_raw_and_cleanup_unknown(tmp_path, monkeypatch, failure, reason):
    result, state, cleanup, command, sends = parent_simulation(tmp_path, monkeypatch, failure)
    assert result['status'] == 'blocked' and reason in result['reason']
    manifest = json.loads(Path(result['topology_namespace']['manifest_path']).read_bytes())
    assert reason in manifest['error']
    assert manifest['maturity_credit'] is False and manifest['host_clearance_claimed'] is False
    assert (tmp_path / 'raw.jsonl').read_bytes() == state['raw_before']
    assert (tmp_path / 'topology-namespace/child-runtime-result.json').read_bytes() == state['child_before']
    cleanup.assert_called_once()


def test_host_cannot_invoke_production_bridge_by_flag(tmp_path):
    cfg, req = outer_request()
    cfg.update(isolated=True, kernel_proof=True, live_execution_go=True)
    with patch.object(bridge.subprocess, 'Popen', side_effect=AssertionError('NO LAUNCH')):
        result = bridge.run_isolated_topology(cfg, tmp_path, boundary_request=req)
    assert result['status'] == 'blocked'
    assert result['topology_namespace']['cleanup']['confirmed'] is False
    manifest = json.loads(Path(result['topology_namespace']['manifest_path']).read_text())
    assert manifest['nested_controller_binding'] is None


@pytest.mark.parametrize('mode', ['nested', 'legacy', 'offline', 'preflight_blocked'])
def test_actual_inner_dispatch_is_narrow_and_keeps_lease_scope(tmp_path, mode):
    cfg, req = outer_request()
    if mode == 'legacy': cfg = dict(cfg, target_id='discovery.late_endpoint')
    if mode == 'offline': cfg['backend'] = 'offline_fake'
    req.update(config=cfg, audit_dir=str(tmp_path / 'audit'), output_dir=str(tmp_path), probe=None)
    outer = identity(1, 'outer', uid=1000)
    runtime = {'events': [], 'status': 'executed'}
    if mode == 'nested':
        runtime['topology_namespace'] = {'raw_pid_scope': 'pid:[nested]',
                                       'nested_controller_binding': {'pid_scope': 'pid:[nested]'}}
    with patch.object(boundary, '_libcall'), patch.object(boundary, '_loopback'), patch.object(
            boundary, '_drop_caps'), patch.object(boundary.signal, 'signal'), patch.object(
            boundary, '_proof'), patch.object(boundary, '_recv', return_value=({'kind': 'execute'}, (), [])), patch.object(
            boundary, '_send'), patch.object(boundary, '_same_source'), patch.object(
            boundary, '_identity', return_value=outer), patch('r2f.runtime.environment.check_environment',
            return_value={'ok': mode != 'preflight_blocked', 'reason': 'not clear'}), patch.object(
            bridge, 'run_isolated_topology', return_value=runtime) as nested, patch(
            'r2f.runtime.ros2_pair.run_pair', return_value=runtime) as legacy:
        assert boundary._inner(Mock(), req) == 0
    leases = json.loads((tmp_path / 'audit/inner-leases.json').read_text())
    if mode == 'nested':
        nested.assert_called_once_with(cfg, tmp_path / 'inner-runtime', boundary_request=req)
        legacy.assert_not_called()
        assert leases['pid_scope'] == 'pid:[nested]' and leases['outer_pid_scope'] == 'pid:[outer]'
    else:
        nested.assert_not_called()
        assert legacy.call_count == (mode != 'preflight_blocked')
        assert leases == {'pid_scope': 'pid:[outer]', 'lock_root': cfg['lock_root'], 'receipts': []}


def test_cleanup_kills_pidfd_bound_init_even_after_wrapper_exit(monkeypatch):
    proc = Mock(pid=19, returncode=0)
    proc.poll.return_value = 0
    exited = iter([False, True])
    monkeypatch.setattr(boundary, '_exited', lambda fd: next(exited))
    signal_fd = Mock()
    killpg = Mock()
    monkeypatch.setattr(boundary, '_signal_pidfd', signal_fd)
    monkeypatch.setattr(bridge.os, 'killpg', killpg)
    monkeypatch.setattr(bridge.select, 'select', Mock())
    monkeypatch.setattr(bridge.os, 'waitpid', Mock(side_effect=ChildProcessError))
    result = bridge._cleanup(proc, 13, {'term_grace_sec': .1, 'kill_wait_sec': .1})
    assert result['confirmed'] is True
    signal_fd.assert_called_once_with(13, bridge.signal.SIGKILL)
    killpg.assert_not_called()  # wrapper is dead; never signal a potentially reused PGID


def test_cleanup_escalates_timeout_for_owned_group_and_pid1(monkeypatch):
    proc = Mock(pid=19, returncode=None)
    proc.poll.return_value = None
    def wait(timeout):
        if proc.wait.call_count == 1: raise bridge.subprocess.TimeoutExpired('owned-unshare', timeout)
        proc.returncode = -9
    proc.wait.side_effect = wait
    monkeypatch.setattr(boundary, '_exited', lambda fd: True)
    monkeypatch.setattr(bridge.os, 'waitpid', Mock(side_effect=ChildProcessError))
    killpg, signal_fd = Mock(), Mock()
    monkeypatch.setattr(bridge.os, 'killpg', killpg)
    monkeypatch.setattr(boundary, '_signal_pidfd', signal_fd)
    result = bridge._cleanup(proc, 13, {'term_grace_sec': .1, 'kill_wait_sec': .1})
    assert result['confirmed'] is True
    assert killpg.call_args_list == [((19, bridge.signal.SIGTERM),), ((19, bridge.signal.SIGKILL),)]
    signal_fd.assert_called_once_with(13, bridge.signal.SIGKILL)


def test_missing_init_binding_cannot_claim_cleanup(monkeypatch):
    proc = Mock(pid=19, returncode=0)
    proc.poll.return_value = 0
    monkeypatch.setattr(bridge.os, 'waitpid', Mock(side_effect=ChildProcessError))
    result = bridge._cleanup(proc, None, {'term_grace_sec': .1, 'kill_wait_sec': .1})
    assert result['confirmed'] is False and result['controller_exited'] is False


def child_simulation(tmp_path, monkeypatch, failure=None, *, request_change=None, stale_hash=False):
    side = tmp_path / 'topology-namespace'
    side.mkdir()
    cfg = formal_topology_config()
    req = {'bridge_id': '1' * 32, 'output_root': str(tmp_path),
           'config': cfg, 'config_sha256': boundary._hash(cfg),
           'source_manifest': cfg['source_manifest'], 'outer': identity(1, 'outer', uid=1000),
           'options': {'startup_timeout_sec': 1, 'kill_wait_sec': 1}}
    path = side / 'request.json'
    boundary._write(path, req)
    supplied_request_sha = bridge._sha(path)
    if request_change is not None:
        request_change(req)
        path.write_text(json.dumps(req, sort_keys=True) + '\n')
        if not stale_hash:
            supplied_request_sha = bridge._sha(path)
    sock = Mock()
    monkeypatch.setattr(bridge.socket, 'socket', lambda **kwargs: sock)
    monkeypatch.setattr(boundary, '_dumpable', Mock())
    monkeypatch.setattr(boundary, '_same_source', Mock(return_value=req['source_manifest']))
    monkeypatch.setattr(boundary, '_nested_proc_scope', Mock(return_value={'identity': identity(1, 'nested')}))
    # Real harmless FDs, not namespace evidence. No processes launched.
    real_open = bridge.os.open
    monkeypatch.setattr(bridge.os, 'pidfd_open', lambda pid: real_open('/dev/null', bridge.os.O_RDONLY))
    monkeypatch.setattr(bridge.os, 'open', lambda *args: real_open('/dev/null', bridge.os.O_RDONLY))
    responses = [({'kind': 'execute', 'config_sha256': req['config_sha256']}, (0, 0, 0), []),
                 ({'kind': 'result_ack'}, (0, 0, 0), [])]
    if failure == 'credentials': responses[0] = (responses[0][0], (2, 1000, 1000), [])
    if failure == 'hash': responses[0][0]['config_sha256'] = '0' * 64
    if failure == 'ack': responses[1] = (responses[1][0], (2, 0, 0), [])
    monkeypatch.setattr(boundary, '_recv', Mock(side_effect=responses))
    sends = Mock()
    monkeypatch.setattr(boundary, '_send', sends)
    runtime = {'status': 'blocked', 'reason': 'original_noGO', 'events': [], 'raw_path': None}
    def run(cfg_arg, out):
        assert cfg_arg == cfg and out == tmp_path
        assert 'live_execution_go' not in cfg_arg
        assert type(bridge.current_child_context()) is bridge._ChildContext
        if failure == 'source_after':
            boundary._same_source.side_effect = boundary.BoundaryError('source after drift')
        return runtime
    runtime_call = Mock(side_effect=run)
    monkeypatch.setattr('r2f.runtime.ros2_pair.run_pair', runtime_call)
    status = bridge._child(str(path), supplied_request_sha, 99)
    assert bridge.current_child_context() is None
    sock.close.assert_called_once()
    return status, side, sends, runtime_call, runtime


def test_child_runs_canonical_callable_only_after_authenticated_ack(tmp_path, monkeypatch):
    status, side, sends, runtime_call, runtime = child_simulation(tmp_path, monkeypatch)
    assert status == 0 and runtime_call.call_count == 1
    persisted = json.loads((side / 'child-runtime-result.json').read_text())
    assert persisted == runtime and 'topology_namespace' not in persisted
    envelope = json.loads((side / 'child-envelope.json').read_text())
    assert bridge._load_ref(envelope['inner_result_ref'], tmp_path) == runtime
    assert envelope['raw_ref'] is None and envelope['inner_lease_scope'] is None
    assert [call.args[1]['kind'] for call in sends.call_args_list] == ['ready', 'result']
    assert sends.call_args.args[1]['envelope_sha256'] == bridge._sha(side / 'child-envelope.json')


@pytest.mark.parametrize('failure,reason,called', [
    ('credentials', 'execute_not_authenticated', False), ('hash', 'execute_not_authenticated', False),
    ('ack', 'not_acknowledged', True), ('source_after', 'source after drift', True),
])
def test_child_preserves_failure_without_running_unadmitted_workload(tmp_path, monkeypatch, failure, reason, called):
    status, side, sends, runtime_call, runtime = child_simulation(tmp_path, monkeypatch, failure)
    assert status == 127 and runtime_call.call_count == int(called)
    assert reason in json.loads((side / 'child-failure.json').read_text())['error']
    if called:
        assert json.loads((side / 'child-runtime-result.json').read_text()) == runtime
    else:
        assert not (side / 'child-runtime-result.json').exists()


@pytest.mark.parametrize('failure', [None, 'leases', 'worker'])
def test_canonical_pair_consumes_child_context_without_any_ros_process(tmp_path, monkeypatch, failure):
    """Injected backend proves call sites only, never kernel/ROS qualification."""
    from r2f.runtime import topology_pair
    from r2f.runtime.ros2_pair import run_pair
    from r2f.runtime.topology_worker import OfflineTopologyBackend
    class ExplicitInjectedBackend(OfflineTopologyBackend):
        real_ros = True  # branch double only; all backend events remain fixtures
        backend_name = ROS_BACKEND
    cfg = formal_topology_config()
    cfg['live_execution_go'] = True  # explicit TEST execution intent, not bridge injection
    lease = MagicMock(receipts=[{}, {}, {}])
    monkeypatch.setattr('r2f.campaign.identity.source_manifest', lambda repo: cfg['source_manifest'])
    monkeypatch.setattr('r2f.runtime.ros2_pair.acquire_pair_environment',
                        lambda *args, **kwargs: (lease, {'ok': True, 'source': cfg['source_manifest']}, {}))
    monkeypatch.setattr('r2f.runtime.ros2_pair.verify_pair_lease', lambda *args: lease.receipts)
    monkeypatch.setattr(topology_pair, 'TopologyEnvironmentObservation',
                        lambda *args: SimpleNamespace(observe=lambda *a, **kw: {
                            'empty_inventory_verified': False, 'measurement_ref': None,
                            'isolation_ref': None, 'limitations': ['test_unmeasured'],
                            'network_neutral_verified': False}))
    context = Mock()
    if failure == 'leases': context.leases_ready.side_effect = TopologyBackendBlocked('test lease rejection')
    if failure == 'worker': context.worker_created.side_effect = TopologyBackendBlocked('test worker rejection')
    monkeypatch.setattr(bridge, 'current_child_context', lambda: context)
    monkeypatch.setattr(bridge.subprocess, 'Popen', Mock(side_effect=AssertionError('NO ROS/NS PROCESS')))
    backend = ExplicitInjectedBackend()
    # Direct call-site test: canonical route deliberately refuses this fake real backend.
    # Do not weaken that route admission just to exercise hooks with doubles.
    result = topology_pair.run_topology(cfg["topology_program"], cfg, tmp_path, backend=backend)
    assert context.leases_ready.call_count == 1, result.get("reason")
    assert context.leases_ready.call_args.args[0] is lease
    assert context.leases_ready.call_args.args[2] == tmp_path
    if failure == 'leases':
        context.worker_created.assert_not_called()
        assert 'test lease rejection' in result['reason']
    elif failure == 'worker':
        context.worker_created.assert_called_once()
        assert 'test worker rejection' in result['reason']
    else:
        assert context.worker_created.call_count >= 2
    assert result['cleanup_receipt']['closed'] and backend.closed
    assert not result['candidate_eligible'] and not result['sealed']
    lease.__exit__.assert_called_once()


def test_actual_producer_serializes_revision2_request_bridge_id(tmp_path, monkeypatch):
    """Use producer bytes, not a hand-built interface request/sample."""
    result, state, cleanup, command, sends = parent_simulation(tmp_path, monkeypatch)
    descriptor = result['topology_namespace']
    manifest = bridge._load_ref(bridge._ref(descriptor['manifest_path']), tmp_path)
    request = bridge._load_ref(manifest['request_ref'], tmp_path)
    assert set(request) == {'bridge_id', 'config', 'config_sha256', 'output_root',
                            'source_manifest', 'outer', 'options'}
    assert request['bridge_id'] == manifest['bridge_id'] == descriptor['bridge_id']
    assert bridge.uuid.UUID(hex=request['bridge_id']).hex == request['bridge_id']
    cfg, boundary_request = outer_request()
    assert request['config'] == cfg
    assert request['config_sha256'] == boundary._hash(cfg) == manifest['config_sha256']
    assert request['source_manifest'] == manifest['source_manifest']
    assert request['outer'] == manifest['outer_controller']
    assert request['options'] == boundary_request['options']
    assert request['output_root'] == manifest['output_root'] == str(tmp_path)
    assert 'bridge_id' not in request['config'] and 'live_execution_go' not in request['config']
    assert (tmp_path / 'raw.jsonl').read_bytes() == state['raw_before']


@pytest.mark.parametrize('value', [None, '', 'BRIDGE', '0' * 31, '0' * 33,
                                   'f' * 31 + 'g', 'A' * 32, True, 17, {}])
def test_child_rejects_invalid_bridge_id_before_kernel_or_runtime(tmp_path, monkeypatch, value):
    status, side, sends, runtime_call, runtime = child_simulation(
        tmp_path, monkeypatch, request_change=lambda req: req.update(bridge_id=value))
    assert status == 127
    failure = json.loads((side / 'child-failure.json').read_text())
    assert 'topology_namespace_child_bridge_id_invalid' in failure['error']
    runtime_call.assert_not_called()
    boundary._nested_proc_scope.assert_not_called()
    boundary._same_source.assert_not_called()
    sends.assert_not_called()
    assert not (side / 'source-before.json').exists()


def test_child_rejects_absent_bridge_id_before_runtime(tmp_path, monkeypatch):
    status, side, sends, runtime_call, runtime = child_simulation(
        tmp_path, monkeypatch, request_change=lambda req: req.pop('bridge_id'))
    assert status == 127
    assert 'child_bridge_id_invalid' in json.loads((side / 'child-failure.json').read_text())['error']
    runtime_call.assert_not_called()
    boundary._nested_proc_scope.assert_not_called()


def test_request_sha_binds_even_well_formed_tampered_bridge_id(tmp_path, monkeypatch):
    status, side, sends, runtime_call, runtime = child_simulation(
        tmp_path, monkeypatch, request_change=lambda req: req.update(bridge_id='2' * 32), stale_hash=True)
    assert status == 127
    runtime_call.assert_not_called()
    boundary._nested_proc_scope.assert_not_called()
    sends.assert_not_called()
    # Failure occurred before a trusted request existed: do not write through
    # an unauthenticated output_root or invent a child-failure result there.
    assert not (side / 'child-runtime-result.json').exists()
    assert not (side / 'child-failure.json').exists()


def test_actual_producer_revision2_field_inventory_and_acyclic_hashes(tmp_path, monkeypatch):
    result, state, cleanup, command, sends = parent_simulation(tmp_path, monkeypatch)
    descriptor = result['topology_namespace']
    manifest = bridge._load_ref(bridge._ref(descriptor['manifest_path']), tmp_path)
    envelope = bridge._load_ref(manifest['envelope_ref'], tmp_path)
    request = bridge._load_ref(manifest['request_ref'], tmp_path)
    assert set(descriptor) == {'schema', 'bridge_id', 'manifest_path', 'manifest_sha256',
                               'nested_controller_binding', 'outer_controller', 'raw_pid_scope',
                               'inner_lease_scope', 'worker_bindings', 'cleanup'}
    assert set(manifest) == {'schema', 'bridge_id', 'output_root', 'host_clearance_claimed',
                             'maturity_credit', 'error', 'nested_controller_binding', 'raw_pid_scope',
                             'inner_lease_scope', 'worker_bindings', 'parent_process_observations',
                             'outer_controller', 'source_manifest', 'config_sha256', 'request_ref',
                             'command', 'envelope_ref', 'inner_result_ref', 'source_before_ref',
                             'source_after_ref', 'raw_ref', 'cleanup'}
    assert set(envelope) == {'inner_result_ref', 'source_before_ref', 'source_after_ref',
                             'inner_lease_scope', 'worker_bindings', 'raw_ref'}
    assert set(request) == {'bridge_id', 'config', 'config_sha256', 'output_root',
                            'source_manifest', 'outer', 'options'}
    for key in ('nested_controller_binding', 'outer_controller', 'raw_pid_scope',
                'inner_lease_scope', 'worker_bindings', 'cleanup', 'bridge_id'):
        assert descriptor[key] == manifest[key]
    for key in ('inner_result_ref', 'source_before_ref', 'source_after_ref', 'raw_ref'):
        assert envelope[key] == manifest[key]
    for key in ('request_ref', 'envelope_ref', 'inner_result_ref', 'source_before_ref',
                'source_after_ref', 'raw_ref'):
        ref = manifest[key]
        assert set(ref) == {'path', 'sha256', 'kind'}
        assert Path(ref['path']).is_relative_to(tmp_path)
        assert ref['kind'] == ('jsonl' if key == 'raw_ref' else 'json')
        assert bridge._sha(ref['path']) == ref['sha256']
    assert descriptor['manifest_sha256'] == bridge._sha(descriptor['manifest_path'])
    assert 'manifest_sha256' not in manifest
    child = bridge._load_ref(manifest['inner_result_ref'], tmp_path)
    assert 'topology_namespace' not in child
    for scope in (manifest['inner_lease_scope'], *manifest['worker_bindings']):
        assert set(scope['raw_event_ref']) == {'path', 'sha256', 'event_index'}
        assert type(scope['raw_event_ref']['event_index']) is int
    leases = copy.deepcopy(manifest['inner_lease_scope'])
    leases['raw_event_ref'] = leases['raw_event_ref']['event_index']
    assert envelope['inner_lease_scope'] == leases
    workers = copy.deepcopy(manifest['worker_bindings'])
    for worker in workers:
        worker['raw_event_ref'] = worker['raw_event_ref']['event_index']
    assert envelope['worker_bindings'] == workers
