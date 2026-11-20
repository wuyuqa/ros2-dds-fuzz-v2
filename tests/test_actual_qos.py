"""Offline only: mocked public library and endpoint handles, no ROS or DDS."""
import ctypes
import hashlib
import os
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

import pytest

from r2f.runtime import actual_qos as aq


class Handle:
    def __init__(self, pointer=1234):
        self.value = pointer
        self.active = False
        self.events = []

    def __enter__(self):
        self.active = True
        self.events.append('enter')
        return self

    @property
    def pointer(self):
        assert self.active
        self.events.append('pointer')
        return self.value

    def __exit__(self, *args):
        self.events.append('exit')
        self.active = False


class Endpoint:
    def __init__(self, handle):
        self.handle = handle

    @property
    def qos_profile(self):
        raise AssertionError('requested QoS must never be accessed')


def profile():
    value = aq._QoS()
    value.history, value.depth = 1, 27
    value.reliability, value.durability, value.liveliness = 2, 1, 3
    value.deadline.sec, value.deadline.nsec = 2, 3
    value.liveliness_lease_duration.sec = 9223372036
    value.liveliness_lease_duration.nsec = 854775807
    return value


def observe(value=None, kind='writer', pointer=1234, null=False):
    value = profile() if value is None else value
    handle = Handle(pointer)
    def get_actual(arg):
        assert handle.active
        assert arg.value == pointer
        handle.events.append('get_actual')
        return ctypes.POINTER(aq._QoS)() if null else ctypes.pointer(value)
    getter = Mock(side_effect=get_actual)
    library = SimpleNamespace(**{api: getter for api in aq._APIS.values()})
    with patch.object(aq, '_load_mapped_library', return_value=(
            library, {'path': '/mock/librcl.so', 'sha256': 'a' * 64})), \
            patch.object(aq, '_verify_abi', return_value={'verified': True, 'method': 'fixture'}):
        receipt = aq.read_public_endpoint_qos(Endpoint(handle), kind)
    return receipt, handle, getter


@pytest.mark.parametrize('kind', ['writer', 'reader'])
def test_copy_lifecycle_and_receipt(kind):
    value = profile()
    receipt, handle, getter = observe(value, kind)
    assert handle.events == ['enter', 'pointer', 'get_actual', 'exit']
    assert getter.argtypes == [ctypes.c_void_p]
    assert getter.restype == ctypes.POINTER(aq._QoS)
    assert receipt['api'] == aq._APIS[kind]
    assert receipt['qos'] == dict(history='keep_last', depth=27,
        reliability='best_effort', durability='transient_local', liveliness='manual_by_topic',
        deadline_ns=2000000003, liveliness_lease_duration_ns=(1 << 63) - 1)
    value.depth = 999
    assert receipt['qos']['depth'] == 27
    assert receipt['schema'] == 'r2f.public-endpoint-qos/v1'
    assert receipt['status'] == 'observed'
    assert receipt['pid'] == os.getpid()
    assert isinstance(receipt['sampled_monotonic_ns'], int)
    assert receipt['abi']['verified'] is True


@pytest.mark.parametrize('pointer', [0, None, -1, True, '123', 1 << 65])
def test_bad_endpoint_pointer(pointer):
    with pytest.raises(aq.ActualQoSReadbackError, match='pointer'):
        observe(pointer=pointer)


def test_null_readback():
    with pytest.raises(aq.ActualQoSReadbackError, match='NULL'):
        observe(null=True)


@pytest.mark.parametrize('field', list(aq._ENUMS))
@pytest.mark.parametrize('bad', [-1, 999])
def test_invalid_enums(field, bad):
    value = profile()
    setattr(value, field, bad)
    with pytest.raises(aq.ActualQoSReadbackError, match='enum'):
        observe(value)


@pytest.mark.parametrize('field', ['deadline', 'lifespan', 'liveliness_lease_duration'])
@pytest.mark.parametrize('sec,nsec', [(0, 1000000000), ((1 << 64) - 1, 0)])
def test_invalid_time(field, sec, nsec):
    value = profile()
    getattr(value, field).sec, getattr(value, field).nsec = sec, nsec
    with pytest.raises(aq.ActualQoSReadbackError, match='duration'):
        observe(value)


def test_unknown_and_default_values_are_not_config_filled():
    value = profile()
    value.history, value.reliability, value.durability, value.liveliness = 3, 0, 3, 4
    receipt, _, _ = observe(value)
    assert receipt['qos']['history'] == 'unknown'
    assert receipt['qos']['reliability'] == 'system_default'
    assert receipt['qos']['durability'] == 'unknown'
    assert receipt['qos']['liveliness'] == 'unknown'


def test_bad_kind():
    with pytest.raises(aq.ActualQoSReadbackError, match='kind'):
        aq.read_public_endpoint_qos(None, 'publisher')


def test_installed_public_header_abi():
    receipt = aq._verify_abi()
    assert receipt['verified'] is True
    assert receipt['size'] == 88
    assert receipt['offsets']['liveliness_lease_duration'] == 64


def test_unknown_headers_rejected(tmp_path):
    for name in aq._HEADER_SHA256:
        (tmp_path / name).write_text('unknown ABI')
    with patch.object(aq, '_HEADER_ROOT', tmp_path), pytest.raises(
            aq.ActualQoSReadbackError, match='header not allowlisted'):
        aq._verify_abi()


def test_layout_mismatch_rejected():
    with patch.object(aq, '_OFFSETS', {}), pytest.raises(
            aq.ActualQoSReadbackError, match='layout mismatch'):
        aq._verify_abi()


def test_wrong_platform_rejected():
    with patch.object(aq.platform, 'machine', return_value='aarch64'), pytest.raises(
            aq.ActualQoSReadbackError, match='unsupported ABI'):
        aq._verify_abi()


def test_only_noload_library_and_identity(tmp_path):
    path = tmp_path / 'librcl.so'
    path.write_bytes(b'mock ELF identity')
    fake = Mock()
    with patch.object(aq, '_mapped_library_paths', return_value=[str(path)]), \
            patch.object(aq.ctypes, 'CDLL', return_value=fake) as loader:
        library, identity = aq._load_mapped_library()
    assert library is fake
    assert identity == {'path': str(path), 'sha256': hashlib.sha256(path.read_bytes()).hexdigest()}
    loader.assert_called_once_with(str(path), mode=os.RTLD_NOLOAD | os.RTLD_LOCAL | os.RTLD_NOW)


@pytest.mark.parametrize('paths', [[], ['/a/librcl.so', '/b/librcl.so']])
def test_missing_or_ambiguous_library(paths):
    with patch.object(aq, '_mapped_library_paths', return_value=paths), \
            patch.object(aq.ctypes, 'CDLL') as loader, pytest.raises(aq.ActualQoSReadbackError):
        aq._load_mapped_library()
    loader.assert_not_called()


def test_mapped_paths_do_not_select_rclpy_or_rmw():
    lines = ('0-1 r-xp 0 0:0 1 /mock/librcl.so\n'
             '0-1 r-xp 0 0:0 2 /mock/librclpy.so\n'
             '0-1 r-xp 0 0:0 3 /mock/librmw.so\n')
    with patch.object(Path, 'read_text', return_value=lines), \
            patch.object(Path, 'resolve', lambda self, **kw: self):
        assert aq._mapped_library_paths() == ['/mock/librcl.so']


def test_abi_failure_precedes_handle_and_library():
    with patch.object(aq, '_verify_abi', side_effect=aq.ActualQoSReadbackError('ABI failed')), \
            patch.object(aq, '_load_mapped_library') as loader, pytest.raises(
                aq.ActualQoSReadbackError, match='ABI failed'):
        aq.read_public_endpoint_qos(None, 'writer')
    loader.assert_not_called()


def test_handle_failure_wrapped():
    with patch.object(aq, '_verify_abi', return_value={'verified': True}), \
            patch.object(aq, '_load_mapped_library', return_value=(SimpleNamespace(
                rcl_publisher_get_actual_qos=Mock()), {})), pytest.raises(
                    aq.ActualQoSReadbackError, match='AttributeError'):
        aq.read_public_endpoint_qos(None, 'writer')


def test_copy_is_complete_before_handle_exit():
    value = profile()
    class InvalidatingHandle(Handle):
        def __exit__(self, *args):
            value.depth = 777
            value.deadline.sec = 888
            super().__exit__(*args)
    handle = InvalidatingHandle()
    getter = Mock(side_effect=lambda _: ctypes.pointer(value))
    with patch.object(aq, '_verify_abi', return_value={'verified': True}), \
            patch.object(aq, '_load_mapped_library', return_value=(SimpleNamespace(
                rcl_publisher_get_actual_qos=getter), {})):
        receipt = aq.read_public_endpoint_qos(Endpoint(handle), 'writer')
    assert value.depth == 777
    assert receipt['qos']['depth'] == 27
    assert receipt['qos']['deadline_ns'] == 2000000003


def test_getter_exception_releases_handle_and_is_wrapped():
    handle = Handle()
    with patch.object(aq, '_verify_abi', return_value={'verified': True}), \
            patch.object(aq, '_load_mapped_library', return_value=(SimpleNamespace(
                rcl_publisher_get_actual_qos=Mock(side_effect=OSError('mock error'))), {})), \
            pytest.raises(aq.ActualQoSReadbackError, match='OSError: mock error'):
        aq.read_public_endpoint_qos(Endpoint(handle), 'writer')
    assert not handle.active
    assert handle.events == ['enter', 'pointer', 'exit']


def test_deleted_mapping_rejected():
    with patch.object(Path, 'read_text', return_value=(
            '0-1 r-xp 0 0:0 1 /mock/librcl.so (deleted)\n')), pytest.raises(
                aq.ActualQoSReadbackError, match='deleted'):
        aq._mapped_library_paths()
