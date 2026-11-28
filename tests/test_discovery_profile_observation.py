"""Offline CP1 contracts: injected worker boundaries, no ROS or DDS execution."""
import base64

from r2f.runtime.profile_observation import initialize_with_profile_observation
from r2f.runtime.worker import initialize_worker_context
from types import SimpleNamespace
import pytest


CYCLONE = b'<CycloneDDS><Domain Id="any"/></CycloneDDS>'


def test_observes_profile_bytes_before_initialization_without_claiming_adoption(tmp_path):
    profile = tmp_path / 'writer.xml'
    profile.write_bytes(CYCLONE)
    package = tmp_path / 'share/rmw_cyclonedds_cpp/package.xml'
    package.parent.mkdir(parents=True)
    package.write_bytes(b'<package><name>rmw_cyclonedds_cpp</name><version>2.3.4</version></package>')
    env = {'RMW_IMPLEMENTATION': 'rmw_cyclonedds_cpp', 'CYCLONEDDS_URI': str(profile),
           'SECRET_TOKEN': 'must-not-leak'}
    before = dict(env)
    def initialize():
        profile.write_bytes(b'changed during init')
    rmw, receipt = initialize_with_profile_observation(
        environment=env, requested_rmw='rmw_cyclonedds_cpp', initialize=initialize,
        rmw_identifier=lambda: 'rmw_cyclonedds_cpp', package_prefix=lambda name: tmp_path)
    assert rmw == 'rmw_cyclonedds_cpp'
    assert receipt['profile_status'] == 'bytes_observed'
    row = receipt['profiles'][0]
    assert base64.b64decode(row['bytes_base64']) == CYCLONE
    assert row['sha256'] == 'c571f747e051fa1c88dd6d2c055e7988db324a9a09d17d1a65d688e6563ace30'
    assert receipt['packages'][0]['version'] == '2.3.4'
    assert receipt['packages'][0]['basis'] == 'installed_package_manifest_not_loaded_library'
    assert receipt['vendor_effective_settings'] == receipt['semantic_effect'] == 'unknown'
    assert receipt['initialization']['status'] == 'returned'
    assert receipt['initialization']['begin_monotonic_ns'] <= receipt['initialization']['returned_monotonic_ns']
    assert env == before
    assert 'must-not-leak' not in str(receipt) and 'SECRET_TOKEN' not in str(receipt)


def test_worker_boundary_emits_process_local_initialization_receipt(tmp_path):
    profile = tmp_path / 'reader.xml'
    profile.write_bytes(CYCLONE)
    manifest = tmp_path / 'share/rmw_cyclonedds_cpp/package.xml'
    manifest.parent.mkdir(parents=True)
    manifest.write_text('<package><name>rmw_cyclonedds_cpp</name><version>0.1-test</version></package>')
    calls, events = [], []
    worker = SimpleNamespace(
        init=lambda *, context: calls.append(('init', context)),
        get_rmw_implementation_identifier=lambda: 'rmw_cyclonedds_cpp')
    context = object()
    rmw = initialize_worker_context(
        worker, context, lambda event, **fields: events.append((event, fields)),
        environment={'RMW_IMPLEMENTATION': 'rmw_cyclonedds_cpp', 'CYCLONEDDS_URI': str(profile)},
        package_prefix=lambda name: tmp_path)
    assert rmw == 'rmw_cyclonedds_cpp'
    assert calls == [('init', context)]
    assert [event for event, _ in events] == ['profile_initialization_observation']
    receipt = events[0][1]['observation']
    assert receipt['initialization']['status'] == 'returned'
    assert receipt['profile_status'] == 'bytes_observed'


def test_missing_profile_is_explicit_and_does_not_prevent_init():
    calls = []
    rmw, receipt = initialize_with_profile_observation(
        environment={'RMW_IMPLEMENTATION': 'rmw_fastrtps_cpp'},
        requested_rmw='rmw_fastrtps_cpp', initialize=lambda: calls.append('init'),
        rmw_identifier=lambda: 'rmw_fastrtps_cpp', package_prefix=lambda name: '/missing')
    assert calls == ['init']
    assert rmw == 'rmw_fastrtps_cpp'
    assert receipt['profile_status'] == 'selector_missing'
    assert receipt['profiles'][0]['status'] == 'selector_missing'
    assert receipt['vendor_effective_settings'] == 'unknown'


def test_mixed_or_unsupported_vendor_and_invalid_shapes_are_explicit():
    _, receipt = initialize_with_profile_observation(
        environment={}, requested_rmw='rmw_unknown', initialize=lambda: None,
        rmw_identifier=lambda: 'rmw_fastrtps_cpp', package_prefix=lambda name: '/missing')
    assert receipt['profile_status'] == 'unsupported_or_mixed_vendor'
    assert receipt['runtime_rmw']['status'] == 'mixed_or_mismatched'
    with pytest.raises(TypeError, match='environment_must_be_string_mapping'):
        initialize_with_profile_observation(environment={'RMW_IMPLEMENTATION': 1}, requested_rmw=None,
            initialize=lambda: None, rmw_identifier=lambda: None, package_prefix=lambda name: '/')


def test_invalid_profile_bytes_are_retained_but_never_called_effective(tmp_path):
    profile = tmp_path / 'broken.xml'
    profile.write_bytes(b'<broken')
    _, receipt = initialize_with_profile_observation(
        environment={'RMW_IMPLEMENTATION': 'rmw_cyclonedds_cpp', 'CYCLONEDDS_URI': str(profile)}, requested_rmw='rmw_cyclonedds_cpp',
        initialize=lambda: None, rmw_identifier=lambda: 'rmw_cyclonedds_cpp',
        package_prefix=lambda name: '/missing')
    assert receipt['profile_status'] == 'invalid_xml'
    assert receipt['profiles'][0]['bytes_base64']
    assert receipt['vendor_effective_settings'] == receipt['semantic_effect'] == 'unknown'


def test_missing_ament_package_is_observation_unknown_not_init_failure():
    class PackageNotFoundError(LookupError):
        pass
    calls = []
    def absent(name):
        raise PackageNotFoundError('private search path')
    rmw, receipt = initialize_with_profile_observation(
        environment={'RMW_IMPLEMENTATION': 'rmw_cyclonedds_cpp'}, requested_rmw='rmw_cyclonedds_cpp', initialize=lambda: calls.append('init'),
        rmw_identifier=lambda: 'rmw_cyclonedds_cpp', package_prefix=absent)
    assert rmw == 'rmw_cyclonedds_cpp' and calls == ['init']
    assert all(row['status'] == 'unavailable' for row in receipt['packages'])
    assert 'private search path' not in str(receipt)


def test_rmw_identifier_failure_keeps_original_failure():
    def failed():
        raise RuntimeError('original identifier failure')
    with pytest.raises(RuntimeError, match='original identifier failure'):
        initialize_with_profile_observation(environment={}, requested_rmw=None,
            initialize=lambda: None, rmw_identifier=failed, package_prefix=lambda name: '/missing')


def test_worker_validates_environment_before_attribute_access():
    with pytest.raises(TypeError, match='environment_must_be_string_mapping'):
        initialize_worker_context(SimpleNamespace(), object(), lambda *args, **kwargs: None,
                                  environment=[], package_prefix=lambda name: '/missing')


@pytest.mark.parametrize('reference', ['file://[bad', 'file://remote/a.xml', 'file:relative.xml',
                                       'https://host/a.xml', '<CycloneDDS/>', '/a.xml;/b.xml'])
def test_unsupported_reference_never_fetches_or_prevents_init(reference):
    calls = []
    _, receipt = initialize_with_profile_observation(
        environment={'RMW_IMPLEMENTATION': 'rmw_cyclonedds_cpp', 'CYCLONEDDS_URI': reference},
        requested_rmw='rmw_cyclonedds_cpp', initialize=lambda: calls.append(True),
        rmw_identifier=lambda: 'rmw_cyclonedds_cpp', package_prefix=lambda name: '/missing')
    assert calls == [True]
    assert receipt['profile_status'] == 'unsupported_reference'


def test_foreign_package_manifest_does_not_claim_selected_version(tmp_path):
    manifest = tmp_path / 'share/rmw_fastrtps_cpp/package.xml'
    manifest.parent.mkdir(parents=True)
    manifest.write_text('<package><name>another_vendor</name><version>99.0</version></package>')
    _, receipt = initialize_with_profile_observation(
        environment={'RMW_IMPLEMENTATION': 'rmw_fastrtps_cpp'}, requested_rmw='rmw_fastrtps_cpp',
        initialize=lambda: None, rmw_identifier=lambda: 'rmw_fastrtps_cpp', package_prefix=lambda name: tmp_path)
    assert receipt['packages'][0]['status'] == 'invalid_manifest'
    assert 'version' not in receipt['packages'][0]


def test_profile_is_read_in_worker_before_init_and_envelope_has_actual_rmw(tmp_path):
    profile = tmp_path / 'fast.xml'
    profile.write_bytes(b'<dds/>')
    env = {'RMW_IMPLEMENTATION': 'rmw_fastrtps_cpp', 'FASTRTPS_DEFAULT_PROFILES_FILE': str(profile)}
    events = []
    worker = SimpleNamespace(init=lambda *, context: profile.unlink(),
                             get_rmw_implementation_identifier=lambda: 'rmw_fastrtps_cpp')
    initialize_worker_context(worker, object(), lambda event, **fields: events.append(fields),
                              environment=env, requested_rmw='rmw_cyclonedds_cpp',
                              package_prefix=lambda name: '/missing')
    row = events[0]
    assert row['rmw_identifier'] == 'rmw_fastrtps_cpp'
    assert row['observation']['configuration_status'] == 'unknown_or_mismatched'
    assert base64.b64decode(row['observation']['profiles'][0]['bytes_base64']) == b'<dds/>'


@pytest.mark.parametrize('environment', [[], 'wrong', 1, {'KEY': None}, {1: 'value'}])
def test_environment_shape_is_rejected_before_init(environment):
    def forbidden():
        pytest.fail('initialization must not run')
    with pytest.raises(TypeError, match='environment_must_be_string_mapping'):
        initialize_with_profile_observation(environment=environment, requested_rmw=None,
            initialize=forbidden, rmw_identifier=lambda: None, package_prefix=lambda name: '/missing')


@pytest.mark.parametrize('requested', [[], {}, 1, True])
def test_requested_rmw_shape_is_rejected_before_init(requested):
    with pytest.raises(TypeError, match='requested_rmw_must_be_string_or_none'):
        initialize_with_profile_observation(environment={}, requested_rmw=requested,
            initialize=lambda: pytest.fail('init forbidden'), rmw_identifier=lambda: None,
            package_prefix=lambda name: '/missing')


@pytest.mark.parametrize('field', ['initialize', 'rmw_identifier', 'package_prefix'])
def test_noncallable_boundary_is_rejected(field):
    kwargs = dict(environment={}, requested_rmw=None, initialize=lambda: None,
                  rmw_identifier=lambda: 'rmw_cyclonedds_cpp', package_prefix=lambda name: '/missing')
    kwargs[field] = None
    with pytest.raises(TypeError, match='observation_callbacks_must_be_callable'):
        initialize_with_profile_observation(**kwargs)


@pytest.mark.parametrize('reference,content,status', [
    ('missing.xml', None, 'unreadable'), ('empty.xml', b'', 'invalid_xml'),
    ('wrong.xml', b'<dds/>', 'vendor_root_mismatch'),
    ('large.xml', b'x' * (1024 * 1024 + 1), 'unsupported_size')])
def test_profile_observation_limits_remain_unknown(tmp_path, reference, content, status):
    path = tmp_path / reference
    if content is not None:
        path.write_bytes(content)
    _, receipt = initialize_with_profile_observation(
        environment={'RMW_IMPLEMENTATION': 'rmw_cyclonedds_cpp', 'CYCLONEDDS_URI': str(path)},
        requested_rmw='rmw_cyclonedds_cpp', initialize=lambda: None,
        rmw_identifier=lambda: 'rmw_cyclonedds_cpp', package_prefix=lambda name: '/missing')
    assert receipt['profile_status'] == status
    assert receipt['semantic_effect'] == receipt['vendor_effective_settings'] == 'unknown'


def test_mixed_selectors_are_not_silently_normalized(tmp_path):
    path = tmp_path / 'profile.xml'
    path.write_bytes(CYCLONE)
    _, receipt = initialize_with_profile_observation(
        environment={'RMW_IMPLEMENTATION': 'rmw_cyclonedds_cpp', 'CYCLONEDDS_URI': path.as_uri(),
                     'FASTRTPS_DEFAULT_PROFILES_FILE': '/other/profile.xml'},
        requested_rmw='rmw_cyclonedds_cpp', initialize=lambda: None,
        rmw_identifier=lambda: 'rmw_cyclonedds_cpp', package_prefix=lambda name: '/missing')
    assert receipt['profile_status'] == 'mixed_or_unsupported_selectors'
    assert receipt['profiles'][0]['status'] == 'bytes_observed'
    assert receipt['process_environment']['FASTRTPS_DEFAULT_PROFILES_FILE'] == '/other/profile.xml'


def test_init_failure_is_preserved_and_no_success_receipt_emitted():
    events = []
    error = RuntimeError('init failed')
    def fail(**kwargs):
        raise error
    worker = SimpleNamespace(init=fail,
                             get_rmw_implementation_identifier=lambda: pytest.fail('must not query failed init'))
    with pytest.raises(RuntimeError) as caught:
        initialize_worker_context(worker, object(), lambda *args, **kwargs: events.append(args),
                                  environment={}, package_prefix=lambda name: '/missing')
    assert caught.value is error and events == []


def test_main_uses_child_receipt_with_existing_raw_identity_before_node(monkeypatch, capsys, tmp_path):
    """Exercise real main entry through ROS/module and existing /proc boundary doubles."""
    import json
    import sys
    from pathlib import Path
    from types import ModuleType
    from r2f.runtime import worker

    class EndAtNode(Exception):
        pass
    config = {'run_id': 'offline-run', 'domain_id': 42, 'rmw': 'rmw_cyclonedds_cpp'}
    profile = tmp_path / 'profile.xml'
    profile.write_bytes(CYCLONE)
    monkeypatch.setenv('RMW_IMPLEMENTATION', 'rmw_cyclonedds_cpp')
    monkeypatch.setenv('CYCLONEDDS_URI', str(profile))
    monkeypatch.setattr(sys, 'argv', ['worker', json.dumps(config), 'reader2'])
    modules = {}
    for name in ('rclpy', 'rclpy.context', 'rclpy.executors', 'rclpy.node', 'rclpy.qos',
                 'std_msgs', 'std_msgs.msg', 'ament_index_python', 'ament_index_python.packages'):
        modules[name] = ModuleType(name)
        monkeypatch.setitem(sys.modules, name, modules[name])
    calls = []
    modules['rclpy'].init = lambda **kwargs: calls.append('init')
    modules['rclpy'].get_rmw_implementation_identifier = lambda: 'rmw_cyclonedds_cpp'
    modules['rclpy.context'].Context = object
    modules['rclpy.executors'].SingleThreadedExecutor = object
    def stop_at_node(*args, **kwargs):
        calls.append('node')
        raise EndAtNode()
    modules['rclpy.node'].Node = stop_at_node
    for name in ('QoSProfile', 'ReliabilityPolicy', 'DurabilityPolicy', 'HistoryPolicy'):
        setattr(modules['rclpy.qos'], name, object)
    modules['std_msgs.msg'].String = object
    modules['ament_index_python.packages'].get_package_prefix = lambda name: tmp_path
    def fake_stat_read(self, *args, **kwargs):
        assert str(self) == '/proc/self/stat'
        return '1 (offline worker) ' + ' '.join(['0'] * 19 + ['12345'])
    monkeypatch.setattr(Path, 'read_text', fake_stat_read)
    monkeypatch.setattr(worker.os, 'getpid', lambda: 321)
    monkeypatch.setattr(worker.os, 'getpgrp', lambda: 320)
    with pytest.raises(EndAtNode):
        worker.main()
    rows = [json.loads(line) for line in capsys.readouterr().out.splitlines()]
    assert len(rows) == 1 and calls == ['init', 'node']
    row = rows[0]
    assert row['event'] == 'profile_initialization_observation'
    assert (row['role'], row['pid'], row['pgid'], row['run_id'], row['domain_id']) == (
        'reader2', 321, 320, 'offline-run', 42)
    assert row['process_start_ticks'] == 12345
    assert row['rmw_identifier'] == 'rmw_cyclonedds_cpp'
    assert row['endpoint_generation'] and row['node_name'].startswith('r2f_reader2_')
    assert row['monotonic_ns'] >= row['observation']['initialization']['returned_monotonic_ns']
    assert row['observation']['profile_status'] == 'bytes_observed'


def test_supplied_environment_receipt_is_snapshot_before_init(tmp_path):
    path = tmp_path / 'before.xml'
    path.write_bytes(CYCLONE)
    env = {'RMW_IMPLEMENTATION': 'rmw_cyclonedds_cpp', 'CYCLONEDDS_URI': str(path)}
    def initialize():
        env['CYCLONEDDS_URI'] = '/changed-during-init.xml'
    _, receipt = initialize_with_profile_observation(environment=env,
        requested_rmw='rmw_cyclonedds_cpp', initialize=initialize,
        rmw_identifier=lambda: 'rmw_cyclonedds_cpp', package_prefix=lambda name: '/missing')
    assert receipt['process_environment']['CYCLONEDDS_URI'] == str(path)


@pytest.mark.parametrize('rmw', ['rmw_cyclonedds_cpp', 'rmw_fastrtps_cpp'])
def test_existing_canonical_role_profiles_are_observed_without_rewriting(tmp_path, rmw):
    from r2f.runtime.discovery_profiles import write_profiles
    records, environments = write_profiles(tmp_path, rmw)
    for role in ('writer', 'reader'):
        before = (tmp_path / 'profiles' / (role + '.xml')).read_bytes()
        _, receipt = initialize_with_profile_observation(
            environment={**environments[role], 'RMW_IMPLEMENTATION': rmw}, requested_rmw=rmw,
            initialize=lambda: None, rmw_identifier=lambda: rmw, package_prefix=lambda name: '/missing')
        assert receipt['profile_status'] == 'bytes_observed'
        observed = receipt['profiles'][0]
        assert observed['sha256'] == records[role]['sha256']
        assert base64.b64decode(observed['bytes_base64']) == before
        assert (tmp_path / 'profiles' / (role + '.xml')).read_bytes() == before


def test_no_selected_rmw_is_explicit_unknown_not_consistent():
    _, receipt = initialize_with_profile_observation(environment={}, requested_rmw=None,
        initialize=lambda: None, rmw_identifier=lambda: 'rmw_cyclonedds_cpp',
        package_prefix=lambda name: pytest.fail('no vendor metadata substitution'))
    assert receipt['configuration_status'] == 'unknown_or_mismatched'
    assert receipt['packages'] == []
    assert receipt['profile_status'] == 'unsupported_or_mixed_vendor'

@pytest.mark.parametrize('encoding', ['NO_SUCH_CODEC', 'UTF-32'])
def test_profile_encoding_errors_are_unknown_and_init_still_runs(tmp_path, encoding):
    profile = tmp_path / 'bad-encoding.xml'
    profile.write_bytes(f'<?xml version="1.0" encoding="{encoding}"?><CycloneDDS/>'.encode())
    calls = []
    _, receipt = initialize_with_profile_observation(
        environment={'RMW_IMPLEMENTATION': 'rmw_cyclonedds_cpp', 'CYCLONEDDS_URI': str(profile)},
        requested_rmw='rmw_cyclonedds_cpp', initialize=lambda: calls.append('init'),
        rmw_identifier=lambda: 'rmw_cyclonedds_cpp', package_prefix=lambda name: '/missing')
    assert calls == ['init']
    assert receipt['profiles'][0]['status'] == 'invalid_xml'
    assert receipt['profiles'][0]['sha256']
    assert base64.b64decode(receipt['profiles'][0]['bytes_base64']) == profile.read_bytes()
    assert receipt['vendor_effective_settings'] == receipt['semantic_effect'] == 'unknown'


def test_package_manifest_is_bounded_regular_and_nonblocking(tmp_path, monkeypatch):
    from r2f.runtime import profile_observation as observation
    from r2f.runtime.profile_observation import _package_identity
    import os

    manifest = tmp_path / 'share/rmw_cyclonedds_cpp/package.xml'
    manifest.parent.mkdir(parents=True)
    manifest.write_bytes(b'<package><name>rmw_cyclonedds_cpp</name><version>1</version></package>')
    real_open = observation.os.open
    opened = []
    def inspect_open(path, flags, *args, **kwargs):
        opened.append(flags)
        assert flags & os.O_NONBLOCK
        return real_open(path, flags, *args, **kwargs)
    monkeypatch.setattr(observation.os, 'open', inspect_open)
    monkeypatch.setattr(type(manifest), 'read_bytes', lambda self: pytest.fail('unbounded read_bytes'))
    assert _package_identity('rmw_cyclonedds_cpp', lambda name: tmp_path)['status'] == 'observed'
    assert opened

    manifest.write_bytes(b'x' * (1024 * 1024 + 1))
    row = _package_identity('rmw_cyclonedds_cpp', lambda name: tmp_path)
    assert row['status'] == 'unsupported_size'
    assert manifest.stat().st_size == 1024 * 1024 + 1

    manifest.unlink()
    os.mkfifo(manifest)
    row = _package_identity('rmw_cyclonedds_cpp', lambda name: tmp_path)
    assert row['status'] == 'unsupported_file'


def test_package_identity_does_not_swallow_unexpected_callback_failures():
    from r2f.runtime.profile_observation import _package_identity
    error = AssertionError('callback bug')
    def fail(name):
        raise error
    with pytest.raises(AssertionError) as caught:
        _package_identity('rmw_cyclonedds_cpp', fail)
    assert caught.value is error


@pytest.mark.parametrize('failure_point', ['identifier', 'event'])
def test_worker_shuts_down_initialized_context_once_on_observation_failure(failure_point):
    calls = []
    context = SimpleNamespace(shutdown=lambda: calls.append('shutdown'))
    original = RuntimeError(f'{failure_point} failure')
    def identifier():
        if failure_point == 'identifier':
            raise original
        return 'rmw_cyclonedds_cpp'
    def emit(*args, **kwargs):
        if failure_point == 'event':
            raise original
    worker = SimpleNamespace(init=lambda *, context: calls.append(('init', context)),
                             get_rmw_implementation_identifier=identifier)
    with pytest.raises(RuntimeError) as caught:
        initialize_worker_context(worker, context, emit, environment={},
                                  package_prefix=lambda name: '/missing')
    assert caught.value is original
    assert calls == [('init', context), 'shutdown']
