"""Copy actual endpoint QoS through public rcl APIs, never requested QoS.

Supported ABI: ROS Jazzy's exact allowlisted public rmw headers on Linux
x86_64 SysV LP64 (standard enum ABI, no packing/short-enum overrides).
Header bytes and ctypes primitive/layout invariants are checked on every read.
This is a header/ABI proof, not a compiler probe or live RMW qualification.
Only an already mapped librcl.so is opened, using RTLD_NOLOAD. No ROS import,
endpoint creation, graph query, vendor pointer, or configuration fallback.
"""
from __future__ import annotations

import ctypes
import hashlib
import os
from pathlib import Path
import platform
import re
import sys
import time


class ActualQoSReadbackError(RuntimeError):
    """Public actual QoS could not be safely observed (no fallback)."""


class _Time(ctypes.Structure):
    _fields_ = [('sec', ctypes.c_uint64), ('nsec', ctypes.c_uint64)]


class _QoS(ctypes.Structure):
    _fields_ = [
        ('history', ctypes.c_int), ('depth', ctypes.c_size_t),
        ('reliability', ctypes.c_int), ('durability', ctypes.c_int),
        ('deadline', _Time), ('lifespan', _Time), ('liveliness', ctypes.c_int),
        ('liveliness_lease_duration', _Time),
        ('avoid_ros_namespace_conventions', ctypes.c_bool),
    ]


_HEADER_ROOT = Path('/opt/ros/jazzy/include/rmw/rmw')
# Full-file hashes cover declarations, enumeration values, includes and macros;
# changes of even comments fail closed until independently reviewed.
_HEADER_SHA256 = {
    'types.h': 'c1e7be9e420d3c32cf6b24db62c3fbd6b97d0518811f6f4677db09d835cf3830',
    'time.h': '5a59df5de3ef1d4a97b9f7fdc7351118b825e0353d77937aaf9e3c63b8f18d32',
}
_OFFSETS = dict(history=0, depth=8, reliability=16, durability=20,
                deadline=24, lifespan=40, liveliness=56,
                liveliness_lease_duration=64, avoid_ros_namespace_conventions=80)
_APIS = {'writer': 'rcl_publisher_get_actual_qos',
         'reader': 'rcl_subscription_get_actual_qos'}
_ENUMS = {
    'history': ('system_default', 'keep_last', 'keep_all', 'unknown'),
    'reliability': ('system_default', 'reliable', 'best_effort', 'unknown', 'best_available'),
    'durability': ('system_default', 'transient_local', 'volatile', 'unknown', 'best_available'),
    'liveliness': ('system_default', 'automatic', 'manual_by_node', 'manual_by_topic',
                   'unknown', 'best_available'),
}


def _verify_abi() -> dict:
    if (sys.platform != 'linux' or platform.machine() != 'x86_64'
            or sys.byteorder != 'little'):
        raise ActualQoSReadbackError('unsupported ABI: requires Linux x86_64 little-endian LP64')
    primitives = {
        'pointer': (ctypes.sizeof(ctypes.c_void_p), ctypes.alignment(ctypes.c_void_p)),
        'long': (ctypes.sizeof(ctypes.c_long), ctypes.alignment(ctypes.c_long)),
        'size_t': (ctypes.sizeof(ctypes.c_size_t), ctypes.alignment(ctypes.c_size_t)),
        'enum_int': (ctypes.sizeof(ctypes.c_int), ctypes.alignment(ctypes.c_int)),
        'uint64': (ctypes.sizeof(ctypes.c_uint64), ctypes.alignment(ctypes.c_uint64)),
        'bool': (ctypes.sizeof(ctypes.c_bool), ctypes.alignment(ctypes.c_bool)),
    }
    expected = dict(pointer=(8, 8), long=(8, 8), size_t=(8, 8),
                    enum_int=(4, 4), uint64=(8, 8), bool=(1, 1))
    offsets = {name: getattr(_QoS, name).offset for name, _ in _QoS._fields_}
    if (primitives != expected or offsets != _OFFSETS
            or ctypes.sizeof(_QoS) != 88 or ctypes.alignment(_QoS) != 8
            or ctypes.sizeof(_Time) != 16 or ctypes.alignment(_Time) != 8
            or _Time.sec.offset != 0 or _Time.nsec.offset != 8):
        raise ActualQoSReadbackError('ABI layout mismatch')
    headers = {}
    for name, expected_hash in _HEADER_SHA256.items():
        path = _HEADER_ROOT / name
        digest = hashlib.sha256(path.read_bytes()).hexdigest()
        if digest != expected_hash:
            raise ActualQoSReadbackError(f'ABI header not allowlisted: {path} sha256={digest}')
        headers[name] = {'path': str(path), 'sha256': digest}
    return {'verified': True, 'method': 'exact-public-headers+sysv-lp64+ctypes-layout',
            'platform': 'linux-x86_64-lp64', 'headers': headers,
            'size': 88, 'alignment': 8, 'offsets': offsets,
            'time_size': 16, 'time_offsets': {'sec': 0, 'nsec': 8},
            'primitive_layouts': primitives,
            'support': 'standard SysV enum ABI; no packing or short-enum overrides'}


def _mapped_library_paths() -> list[str]:
    paths = set()
    for line in Path('/proc/self/maps').read_text().splitlines():
        parts = line.split(None, 5)
        if len(parts) != 6:
            continue
        path = parts[5]
        if re.fullmatch(r'librcl\.so(?:\.\d+)*', Path(path).name):
            if not Path(path).is_absolute() or path.endswith(' (deleted)'):
                raise ActualQoSReadbackError('invalid/deleted mapped rcl library')
            paths.add(str(Path(path).resolve(strict=True)))
        elif 'librcl.so' in path and path.endswith(' (deleted)'):
            raise ActualQoSReadbackError('mapped rcl library has been deleted')
    return sorted(paths)


def _load_mapped_library():
    paths = _mapped_library_paths()
    if len(paths) != 1:
        raise ActualQoSReadbackError(f'expected one already mapped librcl.so, found {len(paths)}')
    if not hasattr(os, 'RTLD_NOLOAD'):
        raise ActualQoSReadbackError('RTLD_NOLOAD unavailable')
    path = paths[0]
    identity = {'path': path, 'sha256': hashlib.sha256(Path(path).read_bytes()).hexdigest()}
    library = ctypes.CDLL(path, mode=os.RTLD_NOLOAD | os.RTLD_LOCAL | os.RTLD_NOW)
    return library, identity


def _duration(value: _Time, field: str) -> int:
    # Accept normalized infinite duration (INT64_MAX ns); reject malformed and
    # out-of-range values instead of silently saturating or importing config.
    total = int(value.sec) * 1_000_000_000 + int(value.nsec)
    if value.nsec >= 1_000_000_000 or total > (1 << 63) - 1:
        raise ActualQoSReadbackError(f'invalid actual QoS duration: {field}')
    return total


def _project(qos: _QoS) -> dict:
    result = {}
    for field, names in _ENUMS.items():
        value = int(getattr(qos, field))
        if not 0 <= value < len(names):
            raise ActualQoSReadbackError(f'invalid actual QoS enum: {field}={value}')
        result[field] = names[value]
    result['depth'] = int(qos.depth)
    result['deadline_ns'] = _duration(qos.deadline, 'deadline')
    result['liveliness_lease_duration_ns'] = _duration(
        qos.liveliness_lease_duration, 'liveliness_lease_duration')
    _duration(qos.lifespan, 'lifespan')  # Validate even the unprojected public member.
    return result


def read_public_endpoint_qos(endpoint, kind: str) -> dict:
    """Observe and copy rcl-owned QoS while the endpoint handle is held alive.

    QoS enum fields use lowercase public enum suffixes (e.g. ``reliable``).
    Null/invalid data, absent mappings, unsupported ABI and all other read errors
    raise ActualQoSReadbackError; no receipt is emitted on failure.
    """
    try:
        if kind not in _APIS:
            raise ActualQoSReadbackError(f'unsupported endpoint kind: {kind!r}')
        abi = _verify_abi()
        library, identity = _load_mapped_library()
        api = _APIS[kind]
        getter = getattr(library, api)
        getter.argtypes = [ctypes.c_void_p]
        getter.restype = ctypes.POINTER(_QoS)
        # rclpy Destroyable.__enter__ acquires the use count but returns None.
        # Keep the handle object itself; never depend on __enter__'s return value.
        handle = endpoint.handle
        with handle:
            pointer = handle.pointer
            if (not isinstance(pointer, int) or isinstance(pointer, bool)
                    or not 0 < pointer <= ctypes.c_void_p(-1).value):
                raise ActualQoSReadbackError('invalid/null endpoint handle pointer')
            actual = getter(ctypes.c_void_p(pointer))
            if not actual:
                raise ActualQoSReadbackError(f'{api} returned NULL')
            # Copy all bytes before releasing the handle; never retain rcl memory.
            copied = _QoS.from_buffer_copy(ctypes.string_at(actual, ctypes.sizeof(_QoS)))
            qos = _project(copied)
            sampled = time.monotonic_ns()
        return {'schema': 'r2f.public-endpoint-qos/v1', 'status': 'observed',
                'api': api, 'qos': qos, 'sampled_monotonic_ns': sampled,
                'pid': os.getpid(), 'library': identity, 'abi': abi}
    except ActualQoSReadbackError:
        raise
    except Exception as exc:
        raise ActualQoSReadbackError(f'public actual QoS read failed: {type(exc).__name__}: {exc}') from exc
