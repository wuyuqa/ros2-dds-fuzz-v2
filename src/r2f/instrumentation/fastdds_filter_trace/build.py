"""Build/probe an opt-in typed evaluate interposer without modifying the vendor.

Run the builder in a fresh session (start_new_session=True). The root must not
exist. This module never sets LD_PRELOAD and never creates a DDS entity. A
successful no-DDS probe establishes the drain ABI, NOT evaluate interception.
"""
from __future__ import annotations

import argparse
import ctypes as C
import hashlib
import json
import os
from pathlib import Path
import platform
import shlex
import subprocess
import sys

from r2f.instrumentation.fastdds_peer.build import build_environment, tree_identity
from r2f.instrumentation.fastdds_peer.probe import validate_manifest as validate_peer_manifest

from . import PAYLOAD_CAPACITY, RECORD_SIZE, SCHEMA_VERSION, STATS_SIZE

SCHEMA = 'r2f.fastdds-filter-trace-build/v1'
EVALUATE_SYMBOL = ('_ZNK8eprosima7fastdds3dds12DDSSQLFilter19DDSFilterExpression8evaluate'
                   'ERKNS_8fastrtps4rtps19SerializedPayload_tERKNS1_14IContentFilter16FilterSampleInfo'
                   'ERKNS5_6GUID_tE')
DEFAULT_ROOT = Path('/home/wuyuqa/.cache/r2f-instrumentation/fastdds-filter-trace-20261120')
ABI = dict(schema_version=SCHEMA_VERSION, record_size=RECORD_SIZE,
           stats_size=STATS_SIZE, payload_capacity=PAYLOAD_CAPACITY)


def digest(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def process_identity():
    return dict(pid=os.getpid(), pgid=os.getpgrp(), sid=os.getsid(0),
                start_ticks=int(Path('/proc/self/stat').read_text().rsplit(')', 1)[1].split()[19]),
                dedicated_process_group=os.getpid() == os.getpgrp())


def trace_command(build, vendor, hook, output, version_script):
    """Inherit the concrete filter translation unit's ABI/config/compiler flags."""
    rows = json.loads((Path(build) / 'compile_commands.json').read_text())
    selected = [row for row in rows if row['file'].endswith('/DDSSQLFilter/DDSFilterExpression.cpp')]
    if len(selected) != 1:
        raise ValueError('evaluate_compile_command_not_unique')
    row = selected[0]
    argv = row.get('arguments') or shlex.split(row['command'])
    flags = []
    index = 1
    while index < len(argv):
        token = argv[index]
        if token in ('-o', '-MF', '-MT', '-MQ'):
            index += 2
        elif token in ('-c', '-MD', '-MMD', '-MP') or token == row['file']:
            index += 1
        else:
            flags.append(token)
            index += 1
    command = [argv[0], *flags, '-shared', '-fPIC', str(hook),
               '-Wl,--no-as-needed', str(vendor), '-Wl,--as-needed',
               '-ldl', '-pthread', '-Wl,-z,defs',
               '-Wl,-rpath,' + str(Path(vendor).parent),
               '-Wl,--version-script=' + str(version_script), '-o', str(output)]
    return command, row


class Record(C.Structure):
    _fields_ = [('schema_version', C.c_uint32), ('struct_size', C.c_uint32),
                ('ordinal', C.c_uint64), ('started_ns', C.c_uint64), ('completed_ns', C.c_uint64),
                ('pid', C.c_uint32), ('decision', C.c_int32), ('status', C.c_uint32),
                ('payload_length', C.c_uint32), ('payload_copied', C.c_uint32),
                ('sequence_high', C.c_int32), ('sequence_low', C.c_uint32),
                ('encapsulation', C.c_uint32), ('reader_guid', C.c_uint8 * 16),
                ('writer_guid', C.c_uint8 * 16), ('payload', C.c_uint8 * PAYLOAD_CAPACITY)]


class Stats(C.Structure):
    _fields_ = [('schema_version', C.c_uint32), ('struct_size', C.c_uint32),
                ('produced', C.c_uint64), ('consumed', C.c_uint64), ('dropped', C.c_uint64),
                ('pending', C.c_uint64), ('resolve_status', C.c_uint32), ('reserved', C.c_uint32)]


RECORD_OFFSETS = dict(schema_version=0, struct_size=4, ordinal=8, started_ns=16,
                      completed_ns=24, pid=32, decision=36, status=40, payload_length=44,
                      payload_copied=48, sequence_high=52, sequence_low=56, encapsulation=60,
                      reader_guid=64, writer_guid=80, payload=96)
STATS_OFFSETS = dict(schema_version=0, struct_size=4, produced=8, consumed=16,
                     dropped=24, pending=32, resolve_status=40, reserved=44)


def _require(condition, reason):
    if not condition:
        raise ValueError(reason)


def no_dds_probe(binary, vendor):
    """Fresh-process, empty-ring ABI probe. Does not invoke evaluate."""
    _require(sys.platform == 'linux' and platform.machine() == 'x86_64'
             and sys.byteorder == 'little' and C.sizeof(C.c_void_p) == 8,
             'unsupported_native_platform')
    for cls, size, offsets in ((Record, RECORD_SIZE, RECORD_OFFSETS), (Stats, STATS_SIZE, STATS_OFFSETS)):
        _require(C.sizeof(cls) == size and {k: getattr(cls, k).offset for k in offsets} == offsets,
                 'ctypes_layout_mismatch')
    binary, vendor = Path(binary).resolve(strict=True), Path(vendor).resolve(strict=True)
    before = {str(p): digest(p) for p in (binary, vendor)}
    lib = C.CDLL(str(binary), mode=os.RTLD_LOCAL | os.RTLD_NOW, use_errno=True)
    pop, stats = lib.r2f_fastdds_filter_trace_pop_v1, lib.r2f_fastdds_filter_trace_stats_v1
    for fn in (pop, stats):
        fn.argtypes, fn.restype = [C.c_void_p, C.c_uint32], C.c_int32
    for fn, cls in ((pop, Record), (stats, Stats)):
        out = cls()
        C.memset(C.byref(out), 0xA5, C.sizeof(out))
        original = bytes(out)
        for wrong in (0, C.sizeof(out) - 1, C.sizeof(out) + 1):
            _require(fn(C.byref(out), wrong) == -1 and bytes(out) == original,
                     'ABI_mismatch_not_rejected_without_write')
        _require(fn(None, C.sizeof(out)) == -1, 'null_output_not_rejected')
    record = Record()
    C.memset(C.byref(record), 0xA5, C.sizeof(record))
    unchanged = bytes(record)
    C.set_errno(73)
    _require(pop(C.byref(record), RECORD_SIZE) == 0 and bytes(record) == unchanged,
             'empty_pop_modified_output')
    _require(C.get_errno() == 73, 'pop_errno_changed')
    out = Stats()
    _require(stats(C.byref(out), STATS_SIZE) == 0 and C.get_errno() == 73, 'stats_failed_or_errno_changed')
    _require(tuple(getattr(out, name) for name, _ in Stats._fields_) == (1, 48, 0, 0, 0, 0, 0, 0),
             'nonempty_or_resolved_without_evaluate')
    expected = {str(binary): binary, str(vendor): vendor}
    mapped = {}
    for line in Path('/proc/self/maps').read_text().splitlines():
        fields = line.split(maxsplit=5)
        if len(fields) != 6:
            continue
        path = fields[5]
        if Path(path).name.startswith(('libfastrtps.so', 'libfastdds.so', 'libr2f_fastdds_filter_trace.so')):
            _require(path in expected, 'foreign_trace_or_vendor_mapping:' + path)
            st = expected[path].stat()
            device = f'{os.major(st.st_dev):02x}:{os.minor(st.st_dev):02x}'
            _require(fields[3] == device and fields[4] == str(st.st_ino), 'mapped_identity_mismatch')
            mapped[path] = dict(path=path, device=device, inode=st.st_ino, sha256=digest(path))
    _require(set(mapped) == set(expected), 'required_binaries_not_mapped')
    _require(before == {str(p): digest(p) for p in (binary, vendor)}, 'binary_changed_during_probe')
    return dict(schema='r2f.fastdds-filter-trace-no-dds-probe/v1', status='passed', abi=ABI,
                process=process_identity(), mapped_binaries=list(mapped.values()),
                checks=['layouts', 'wrong_sizes_without_write', 'null_outputs', 'empty_without_write',
                        'initial_stats', 'errno', 'actual_mapped_identities'],
                evaluate_invoked=False, actual_interposition_verified=False,
                dds_entities_created=False, execution_evidence=False, candidate_eligible=False)


def run_build(*, peer_manifest, peer_manifest_sha256, root=DEFAULT_ROOT):
    root = Path(root).absolute()
    # Exclusive creation: never overwrite a previous build or another owner.
    root.mkdir(parents=True, exist_ok=False)
    commands = []
    receipt = dict(schema=SCHEMA, status='preparing', observation_mode='instrumentation', abi=ABI,
                   commands=commands, verification=dict(compilation='not_run', dds='not_run',
                                                        no_dds_abi='not_run', campaign='not_run'))
    manifest_path = root / 'build-manifest.json'

    def execute(name, command, env, cwd):
        log = root / (name + '.log')
        row = dict(name=name, argv=list(map(str, command)), cwd=str(cwd), log=str(log))
        commands.append(row)
        with log.open('xb') as stream:
            completed = subprocess.run(row['argv'], cwd=cwd, env=env,
                                       stdout=stream, stderr=subprocess.STDOUT)
        row['returncode'] = completed.returncode
        if completed.returncode:
            raise RuntimeError(f'build_step_failed:{name}:{completed.returncode}')

    try:
        receipt['process'] = process_identity()
        _require(receipt['process']['dedicated_process_group'], 'dedicated_process_group_required')
        peer_manifest = Path(peer_manifest).resolve(strict=True)
        peer = validate_peer_manifest(peer_manifest, peer_manifest_sha256)
        receipt['peer_manifest_reference'] = dict(manifest_path=str(peer_manifest), manifest_sha256=peer_manifest_sha256)
        for key in ('source', 'build', 'install'):
            parent = Path(peer[key]).resolve(strict=True)
            _require(not root.resolve().is_relative_to(parent), 'root_inside_peer_' + key)
        source, build = Path(peer['source']), Path(peer['build'])
        _require(digest(build / 'compile_commands.json') == peer['compile_commands_sha256'],
                 'peer_compile_commands_changed')
        _require(tree_identity(source) == peer['source_frozen'], 'peer_source_changed')
        receipt['vendor_source_identity_sha256'] = peer['source_frozen']['sha256']
        receipt['compile_commands_sha256'] = peer['compile_commands_sha256']
        vendor = Path(peer['binaries']['vendor']['path']).resolve(strict=True)
        receipt['vendor_binary'] = dict(path=str(vendor), sha256=digest(vendor))
        hook = Path(__file__).with_name('hook.cpp')
        hook_snapshot = root / 'hook.cpp'
        hook_snapshot.write_bytes(hook.read_bytes())
        receipt['hook_source_sha256'] = digest(hook_snapshot)
        receipt['builder_source_sha256'] = digest(__file__)
        install = root / 'install'
        (install / 'lib').mkdir(parents=True)
        receipt['install'] = str(install)
        output = install / 'lib/libr2f_fastdds_filter_trace.so'
        version_script = root / 'exports.map'
        version_script.write_text('CODEABI_1.0 { global: ' + EVALUATE_SYMBOL +
                                  '; r2f_fastdds_filter_trace_pop_v1; r2f_fastdds_filter_trace_stats_v1; local: *; };\n')
        command, reference = trace_command(build, vendor, hook_snapshot, output, version_script)
        receipt['vendor_reference_command'] = reference
        compiler = Path(command[0]).resolve(strict=True)
        _require(digest(compiler) == peer['toolchain'].get(str(compiler)), 'peer_compiler_identity_mismatch')
        toolchain = compiler.parents[2]
        env = build_environment(toolchain)
        # LD_LIBRARY_PATH precedes RUNPATH: pin the existing isolated vendor
        # before the ROS dependency directory, without changing the parent env.
        env['LD_LIBRARY_PATH'] = str(vendor.parent) + os.pathsep + env['LD_LIBRARY_PATH']
        receipt['toolchain'] = {str(compiler): digest(compiler)}
        receipt['environment'] = {k: env[k] for k in ('PATH', 'LD_LIBRARY_PATH')}
        receipt['status'] = 'building'
        execute('compiler-version', [compiler, '--version'], env, root)
        execute('trace-build', command, env, reference['directory'])
        receipt['verification']['compilation'] = 'passed'
        receipt['binary'] = dict(path=str(output), sha256=digest(output))
        execute('no-dds-abi', [sys.executable, '-m', __package__ + '.build', '--probe',
                             '--binary', output, '--vendor', vendor], env, root)
        probe = json.loads((root / 'no-dds-abi.log').read_text())
        _require(probe.get('status') == 'passed', 'no_dds_probe_did_not_pass')
        receipt['no_dds_probe'] = probe
        receipt['verification']['no_dds_abi'] = 'passed'
        _require(validate_peer_manifest(peer_manifest, peer_manifest_sha256) == peer,
                 'peer_manifest_changed_during_build')
        _require(tree_identity(source) == peer['source_frozen'], 'peer_source_changed_during_build')
        _require(digest(build / 'compile_commands.json') == peer['compile_commands_sha256'],
                 'peer_compile_commands_changed_during_build')
        _require(digest(hook) == receipt['hook_source_sha256']
                 and digest(__file__) == receipt['builder_source_sha256'], 'trace_source_changed_during_build')
        _require(digest(output) == receipt['binary']['sha256'], 'trace_binary_changed_during_build')
        receipt.update(status='built', actual_interposition_verified=False)
    except Exception as exc:
        receipt.update(status='failed', error=str(exc), error_type=type(exc).__name__)
        raise
    finally:
        manifest_path.write_text(json.dumps(receipt, indent=2, sort_keys=True) + '\n')
    return manifest_path


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--peer-manifest', type=Path)
    parser.add_argument('--peer-manifest-sha256')
    parser.add_argument('--root', type=Path, default=DEFAULT_ROOT)
    parser.add_argument('--probe', action='store_true')
    parser.add_argument('--binary', type=Path)
    parser.add_argument('--vendor', type=Path)
    args = parser.parse_args(argv)
    try:
        if args.probe:
            if args.binary is None or args.vendor is None:
                parser.error('--probe requires --binary and --vendor')
            result = no_dds_probe(args.binary, args.vendor)
        else:
            if args.peer_manifest is None or args.peer_manifest_sha256 is None:
                parser.error('explicit --peer-manifest and --peer-manifest-sha256 required')
            path = run_build(peer_manifest=args.peer_manifest, peer_manifest_sha256=args.peer_manifest_sha256,
                             root=args.root)
            result = dict(manifest_path=str(path), manifest_sha256=digest(path))
        print(json.dumps(result, indent=2, sort_keys=True))
        return 0
    except (OSError, ValueError, RuntimeError, KeyError) as exc:
        print(json.dumps(dict(status='failed', error=str(exc), execution_evidence=False)))
        return 2


if __name__ == '__main__':
    raise SystemExit(main())
