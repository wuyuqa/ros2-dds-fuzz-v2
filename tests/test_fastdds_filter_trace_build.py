"""Offline build/ABI contracts. Native tests require an explicit built manifest.

Set R2F_FILTER_TRACE_TEST_MANIFEST to opt into no-DDS probes. The synthetic
next-function double does NOT prove interception of real vendor filter calls.
"""
import ctypes as C
import json
import os
from pathlib import Path
import shlex
import subprocess
import sys

import pytest

from r2f.instrumentation.fastdds_filter_trace import build as b


def compile_row(tmp_path, count=1, arguments=True):
    source = '/vendor/src/cpp/fastdds/topic/DDSSQLFilter/DDSFilterExpression.cpp'
    argv = ['/toolchain/cxx', '-DFASTDDS_STATISTICS', '-DFASTRTPS_SOURCE', '-std=gnu++11',
            '-I/vendor/include', '-isystem', '/generated/include', '-fPIC', '-O3',
            '-MD', '-MT', 'object-target', '-MF', 'object.d', '-o', 'old.o', '-c', source]
    row = dict(file=source, directory='/vendor/build')
    row['arguments' if arguments else 'command'] = argv if arguments else shlex.join(argv)
    (tmp_path / 'compile_commands.json').write_text(json.dumps([row] * count))
    return row


@pytest.mark.parametrize('arguments', [True, False])
def test_command_inherits_concrete_filter_flags_and_links_exact_vendor(tmp_path, arguments):
    row = compile_row(tmp_path, arguments=arguments)
    command, selected = b.trace_command(tmp_path, '/peer/lib/libfastrtps.so.2.14.7',
                                        '/isolated/hook.cpp', '/isolated/trace.so', '/isolated/exports.map')
    assert selected == row
    assert command[0] == '/toolchain/cxx'
    assert all(flag in command for flag in ('-DFASTDDS_STATISTICS', '-DFASTRTPS_SOURCE',
                                            '-std=gnu++11', '-isystem', '/generated/include', '-O3'))
    assert not set(command) & {'-c', '-MD', '-MT', '-MF', 'object-target', 'old.o', 'object.d', row['file']}
    assert command[-2:] == ['-o', '/isolated/trace.so']
    assert command.index('-Wl,--no-as-needed') < command.index('/peer/lib/libfastrtps.so.2.14.7')
    assert '-ldl' in command and '-pthread' in command and '-Wl,-z,defs' in command
    assert '-Wl,-rpath,/peer/lib' in command
    assert '-Wl,--version-script=/isolated/exports.map' in command
    assert '-lfastrtps' not in command  # no ambiguous -L resolution


@pytest.mark.parametrize('count', [0, 2])
def test_concrete_filter_translation_unit_must_be_unique(tmp_path, count):
    compile_row(tmp_path, count=count)
    with pytest.raises(ValueError, match='evaluate_compile_command_not_unique'):
        b.trace_command(tmp_path, '/vendor.so', '/hook.cpp', '/out.so', '/exports.map')


def test_ctypes_layout_matches_every_published_offset():
    for cls, size, offsets in ((b.Record, 4192, b.RECORD_OFFSETS), (b.Stats, 48, b.STATS_OFFSETS)):
        assert C.sizeof(cls) == size
        assert C.alignment(cls) == 8
        assert {name: getattr(cls, name).offset for name in offsets} == offsets
    assert b.ABI == dict(schema_version=1, record_size=4192, stats_size=48, payload_capacity=4096)


def test_existing_root_never_overwritten(tmp_path):
    sentinel = tmp_path / 'sentinel'
    sentinel.write_bytes(b'owner')
    with pytest.raises(FileExistsError):
        b.run_build(peer_manifest='/missing', peer_manifest_sha256='0' * 64, root=tmp_path)
    assert sentinel.read_bytes() == b'owner'
    assert not (tmp_path / 'build-manifest.json').exists()


def test_shared_process_group_rejected_with_failure_receipt(tmp_path, monkeypatch):
    monkeypatch.setattr(b, 'process_identity', lambda: dict(dedicated_process_group=False))
    root = tmp_path / 'new-root'
    with pytest.raises(ValueError, match='dedicated_process_group_required'):
        b.run_build(peer_manifest='/missing', peer_manifest_sha256='0' * 64, root=root)
    manifest = json.loads((root / 'build-manifest.json').read_text())
    assert manifest['status'] == 'failed'
    assert manifest['commands'] == []
    assert manifest['verification']['compilation'] == 'not_run'
    assert 'binary' not in manifest


def fake_peer(tmp_path, monkeypatch, fail=None):
    peer = tmp_path / 'peer'
    source, build, install = (peer / name for name in ('source', 'build', 'install'))
    for path in (source, build, install / 'lib'):
        path.mkdir(parents=True)
    vendor = install / 'lib/libfastrtps.so.2.14.7'
    vendor.write_bytes(b'vendor')
    compiler = tmp_path / 'toolchain/usr/bin/cxx'
    compiler.parent.mkdir(parents=True)
    compiler.write_bytes(b'compiler')
    compile_row(build)
    rows = json.loads((build / 'compile_commands.json').read_text())
    rows[0]['arguments'][0] = str(compiler)
    rows[0]['directory'] = str(build)
    (build / 'compile_commands.json').write_text(json.dumps(rows))
    manifest = dict(source=str(source), build=str(build), install=str(install),
                    source_frozen=b.tree_identity(source),
                    compile_commands_sha256=b.digest(build / 'compile_commands.json'),
                    binaries=dict(vendor=dict(path=str(vendor), sha256=b.digest(vendor))),
                    toolchain={str(compiler): b.digest(compiler)})
    path = peer / 'manifest.json'
    path.write_text(json.dumps(manifest))
    monkeypatch.setattr(b, 'validate_peer_manifest', lambda *args: manifest)
    calls = []

    def run(command, **kwargs):
        from types import SimpleNamespace
        calls.append((command, kwargs))
        if '--version' in command:
            step = 'version'
        elif '--probe' in command:
            step = 'probe'
            kwargs['stdout'].write(json.dumps(dict(status='passed')).encode())
        else:
            step = 'compile'
            Path(command[-1]).write_bytes(b'trace-binary')
        return SimpleNamespace(returncode=9 if step == fail else 0)

    monkeypatch.setattr(b.subprocess, 'run', run)
    monkeypatch.setattr(b, 'process_identity', lambda: dict(dedicated_process_group=True))
    return path, manifest, calls


def test_build_receipt_pins_vendor_and_child_only_library_order(tmp_path, monkeypatch):
    peer, expected, calls = fake_peer(tmp_path, monkeypatch)
    monkeypatch.setenv('LD_PRELOAD', '/foreign.so')
    monkeypatch.setenv('LD_AUDIT', '/audit.so')
    parent = dict(os.environ)
    path = b.run_build(peer_manifest=peer, peer_manifest_sha256=b.digest(peer), root=tmp_path / 'root')
    out = json.loads(path.read_text())
    assert out['schema'] == 'r2f.fastdds-filter-trace-build/v1'
    assert out['status'] == 'built'
    assert out['binary']['sha256'] == b.digest(out['binary']['path'])
    assert out['vendor_binary'] == expected['binaries']['vendor']
    assert out['actual_interposition_verified'] is False
    assert out['verification'] == dict(compilation='passed', dds='not_run', no_dds_abi='passed', campaign='not_run')
    assert out['peer_manifest_reference'] == dict(manifest_path=str(peer), manifest_sha256=b.digest(peer))
    for command, kwargs in calls:
        env = kwargs['env']
        assert env['LD_LIBRARY_PATH'].split(os.pathsep)[0] == str(Path(expected['install']) / 'lib')
        assert 'LD_PRELOAD' not in env and 'LD_AUDIT' not in env
    assert dict(os.environ) == parent


@pytest.mark.parametrize('step', ['version', 'compile', 'probe'])
def test_failure_retained_never_claims_success(tmp_path, monkeypatch, step):
    peer, _, calls = fake_peer(tmp_path, monkeypatch, fail=step)
    root = tmp_path / 'root'
    with pytest.raises(RuntimeError, match='build_step_failed'):
        b.run_build(peer_manifest=peer, peer_manifest_sha256=b.digest(peer), root=root)
    out = json.loads((root / 'build-manifest.json').read_text())
    assert out['status'] == 'failed'
    assert out['verification']['no_dds_abi'] == 'not_run'
    assert out['commands'][-1]['returncode'] == 9
    assert out['verification']['compilation'] == ('passed' if step == 'probe' else 'not_run')


def test_source_changed_before_build_refused(tmp_path, monkeypatch):
    peer, manifest, calls = fake_peer(tmp_path, monkeypatch)
    (Path(manifest['source']) / 'foreign').write_bytes(b'dirty')
    with pytest.raises(ValueError, match='peer_source_changed'):
        b.run_build(peer_manifest=peer, peer_manifest_sha256=b.digest(peer), root=tmp_path / 'root')
    assert not calls


def test_wrong_compile_database_refused(tmp_path, monkeypatch):
    peer, manifest, calls = fake_peer(tmp_path, monkeypatch)
    (Path(manifest['build']) / 'compile_commands.json').write_text('[]')
    with pytest.raises(ValueError, match='peer_compile_commands_changed'):
        b.run_build(peer_manifest=peer, peer_manifest_sha256=b.digest(peer), root=tmp_path / 'root')
    assert not calls


@pytest.fixture(scope='module')
def native_manifest():
    value = os.environ.get('R2F_FILTER_TRACE_TEST_MANIFEST')
    if not value:
        pytest.skip('explicit R2F_FILTER_TRACE_TEST_MANIFEST required; no native loading by default')
    path = Path(value).resolve(strict=True)
    manifest = json.loads(path.read_text())
    assert manifest['schema'] == b.SCHEMA and manifest['status'] == 'built'
    assert b.digest(manifest['binary']['path']) == manifest['binary']['sha256']
    assert b.digest(manifest['vendor_binary']['path']) == manifest['vendor_binary']['sha256']
    assert manifest['hook_source_sha256'] == b.digest(Path(b.__file__).with_name('hook.cpp'))
    assert manifest['builder_source_sha256'] == b.digest(b.__file__)
    return path, manifest


def test_native_empty_probe_in_fresh_process(native_manifest):
    path, manifest = native_manifest
    env = dict(os.environ, **manifest['environment'])
    env.pop('LD_PRELOAD', None)
    env.pop('LD_AUDIT', None)
    result = subprocess.run([sys.executable, '-m', 'r2f.instrumentation.fastdds_filter_trace.build',
                             '--probe', '--binary', manifest['binary']['path'],
                             '--vendor', manifest['vendor_binary']['path']],
                            env=env, capture_output=True, text=True, start_new_session=True)
    assert result.returncode == 0, result.stdout + result.stderr
    receipt = json.loads(result.stdout)
    assert receipt['status'] == 'passed'
    assert receipt['actual_interposition_verified'] is False
    assert receipt['evaluate_invoked'] is False
    assert receipt['dds_entities_created'] is False


# A versioned, typed downstream DOUBLE. No actual DDS filter decision is made.
FAKE_NEXT = r'''
#include <fastdds/topic/DDSSQLFilter/DDSFilterExpression.hpp>
#include <cerrno>
namespace eprosima { namespace fastdds { namespace dds { namespace DDSSQLFilter {
bool DDSFilterExpression::evaluate(const IContentFilter::SerializedPayload& p,
    const IContentFilter::FilterSampleInfo& i, const IContentFilter::GUID_t& r) const {
    if (errno != 71 || r.guidPrefix.value[0] != 0x11 ||
        i.sample_identity.writer_guid().guidPrefix.value[0] != 0x22 ||
        i.sample_identity.sequence_number().high != -7 ||
        i.sample_identity.sequence_number().low != 0xf0010203u) throw 99;
    if (p.data && p.length && p.data[0] == 0xee) { errno = 84; throw 37; }
    errno = 83;
    return p.data && p.length && p.data[0] != 0;
}
}}}}
'''
DRIVER = r'''
#include <fastdds/topic/DDSSQLFilter/DDSFilterExpression.hpp>
#include <dlfcn.h>
#include <cerrno>
#include <cstdint>
#include <cstring>
#include <iostream>
#include <thread>
#include <vector>
#include <cstdlib>
using F = eprosima::fastdds::dds::DDSSQLFilter::DDSFilterExpression;
using I = eprosima::fastdds::dds::IContentFilter;
using Fn = bool (*)(const F*, const I::SerializedPayload&, const I::FilterSampleInfo&, const I::GUID_t&);
using Drain = int32_t (*)(void*, uint32_t);
static void require(bool ok) { if (!ok) std::abort(); }
template<class T> T at(const uint8_t* b, size_t offset) { T t; std::memcpy(&t,b+offset,sizeof(t)); return t; }
int main() {
    Fn evaluate = reinterpret_cast<Fn>(dlsym(RTLD_DEFAULT,"EVALUATE_SYMBOL"));
    Drain pop = reinterpret_cast<Drain>(dlsym(RTLD_DEFAULT,"r2f_fastdds_filter_trace_pop_v1"));
    Drain stats = reinterpret_cast<Drain>(dlsym(RTLD_DEFAULT,"r2f_fastdds_filter_trace_stats_v1"));
    require(evaluate && pop && stats);
    auto call = [&](uint8_t first, uint32_t length, bool null_data=false) {
        F f;  // Unconfigured C++ filter object, NOT a DDS entity; only the double uses it.
        I::SerializedPayload p(null_data ? 0 : length);
        if (!null_data && length) { std::memset(p.data,0x5a,length); p.data[0]=first; }
        p.length=length; p.encapsulation=0x1234;
        I::FilterSampleInfo info;
        I::GUID_t writer, reader;
        for(int n=0;n<12;++n) { writer.guidPrefix.value[n]=0x22+n; reader.guidPrefix.value[n]=0x11+n; }
        for(int n=0;n<4;++n) { writer.entityId.value[n]=0x40+n; reader.entityId.value[n]=0x50+n; }
        info.sample_identity.writer_guid(writer);
        auto seq=info.sample_identity.sequence_number(); seq.high=-7; seq.low=0xf0010203u;
        info.sample_identity.sequence_number(seq);
        errno=71;
        try { bool result=evaluate(&f,p,info,reader); require(result==(!null_data && length && first)); require(errno==83); }
        catch(int value) { require(first==0xee && value==37 && errno==84); }
    };
    uint8_t record[4192], s[48];
    call(1,5); call(0,5); call(0xee,5); call(1,5000); call(0,12,true);
    for(uint64_t n=1;n<=5;++n) {
        require(pop(record,sizeof(record))==1);
        require(at<uint32_t>(record,0)==1 && at<uint32_t>(record,4)==4192);
        require(at<uint64_t>(record,8)==n);
        require(at<uint64_t>(record,16)>0 && at<uint64_t>(record,24)>=at<uint64_t>(record,16));
        require(at<int32_t>(record,36)==(n==3?-1:(n==2||n==5?0:1)));
        require(at<uint32_t>(record,40)==(n==3?1u:0u));
        require(at<int32_t>(record,52)==-7 && at<uint32_t>(record,56)==0xf0010203u);
        require(at<uint32_t>(record,60)==0x1234 && record[64]==0x11 && record[80]==0x22);
        for(int k=0;k<12;++k) require(record[64+k]==0x11+k && record[80+k]==0x22+k);
        for(int k=0;k<4;++k) require(record[76+k]==0x50+k && record[92+k]==0x40+k);
        require(at<uint32_t>(record,44)==(n==4?5000u:(n==5?12u:5u)));
        require(at<uint32_t>(record,48)==(n==4?4096u:(n==5?0u:5u)));
        if(n==4) require(record[96]==1 && record[4191]==0x5a);
        if(n==5) for(int k=96;k<4192;++k) require(record[k]==0);
    }
    for(int n=0;n<140;++n) call(1,1);
    require(stats(s,48)==0 && at<uint64_t>(s,8)==145 && at<uint64_t>(s,16)==5 &&
        at<uint64_t>(s,24)==12 && at<uint64_t>(s,32)==128 && at<uint32_t>(s,40)==1);
    for(uint64_t n=6;n<=133;++n) { require(pop(record,4192)==1); require(at<uint64_t>(record,8)==n); }
    require(pop(record,4192)==0);
    call(1,1); require(pop(record,4192)==1 && at<uint64_t>(record,8)==146);
    std::vector<std::thread> threads;
    for(int n=0;n<4;++n) threads.emplace_back([&](){for(int k=0;k<100;++k) call(1,1);});
    for(auto& thread:threads) thread.join();
    require(stats(s,48)==0 && at<uint64_t>(s,8)==546 && at<uint64_t>(s,16)==134 &&
        at<uint64_t>(s,24)==284 && at<uint64_t>(s,32)==128);
    uint64_t last=146;
    while(pop(record,4192)==1) { require(at<uint64_t>(record,8)>last); last=at<uint64_t>(record,8); }
    require(stats(s,48)==0 && at<uint64_t>(s,8)==at<uint64_t>(s,16)+at<uint64_t>(s,24)+at<uint64_t>(s,32));
    std::cout << "synthetic typed forwarding/exception/errno/payload/FIFO/overflow/concurrency passed; no DDS; real vendor call coverage unverified\n";
}
'''.replace('EVALUATE_SYMBOL', b.EVALUATE_SYMBOL)


def test_native_typed_next_double_preserves_semantics_and_bounded_fifo(native_manifest, tmp_path):
    _, manifest = native_manifest
    env = dict(os.environ, **manifest['environment'])
    env.pop('LD_PRELOAD', None)
    env.pop('LD_AUDIT', None)
    original = next(row['argv'] for row in manifest['commands'] if row['name'] == 'trace-build')
    prefix = original[:original.index('-shared')]
    fake, driver = tmp_path / 'fake.cpp', tmp_path / 'driver.cpp'
    fake.write_text(FAKE_NEXT)
    driver.write_text(DRIVER)
    exports = tmp_path / 'exports.map'
    exports.write_text('CODEABI_1.0 { global: ' + b.EVALUATE_SYMBOL + '; local: *; };\n')
    fake_so, executable = tmp_path / 'fake.so', tmp_path / 'driver'
    vendor = manifest['vendor_binary']['path']
    commands = [
        [*prefix, '-shared', '-fPIC', str(fake), vendor, '-Wl,--version-script=' + str(exports),
         '-Wl,-rpath,' + str(Path(vendor).parent), '-o', str(fake_so)],
        [*prefix, str(driver), '-Wl,--no-as-needed', str(fake_so), vendor, '-ldl', '-pthread',
         '-Wl,-rpath,' + str(tmp_path) + ':' + str(Path(vendor).parent), '-o', str(executable)],
    ]
    for command in commands:
        result = subprocess.run(command, env=env, capture_output=True, text=True, start_new_session=True)
        assert result.returncode == 0, result.stdout + result.stderr
    # Exactly one preload: executable's fake DT_NEEDED precedes real vendor.
    env['LD_PRELOAD'] = manifest['binary']['path']
    result = subprocess.run([str(executable)], env=env, capture_output=True, text=True, start_new_session=True)
    assert result.returncode == 0, (result.returncode, result.stdout, result.stderr)
    assert 'synthetic typed forwarding/exception/errno/payload/FIFO/overflow/concurrency passed' in result.stdout
    # Force only the symbol-resolution boundary to fail in a NEW process.
    # The executable exports this test double; the actual hook must terminate,
    # not continue into fake/real evaluate or return an invented decision.
    failing_source = tmp_path / 'resolve_failure.cpp'
    failing_source.write_text(DRIVER.replace('int main() {',
        'extern "C" void* dlvsym(void*, const char*, const char*) noexcept { return nullptr; }\nint main() {'))
    failed_executable = tmp_path / 'resolve_failure'
    command = [*prefix, str(failing_source), '-rdynamic', '-Wl,--no-as-needed', str(fake_so),
               vendor, '-ldl', '-pthread', '-Wl,-rpath,' + str(tmp_path) + ':' + str(Path(vendor).parent),
               '-o', str(failed_executable)]
    compile_env = dict(env)
    compile_env.pop('LD_PRELOAD')
    result = subprocess.run(command, env=compile_env, capture_output=True, text=True, start_new_session=True)
    assert result.returncode == 0, result.stdout + result.stderr
    result = subprocess.run([str(failed_executable)], env=env, capture_output=True, text=True, start_new_session=True)
    assert result.returncode == 127
    assert result.stdout == ''
    assert 'RTLD_NEXT evaluate@CODEABI_1.0 unavailable; terminating' in result.stderr


def test_native_exports_exact_default_version(native_manifest):
    _, manifest = native_manifest
    compiler = next(iter(manifest['toolchain']))
    nm = Path(compiler).with_name('x86_64-linux-gnu-nm')
    env = dict(os.environ, **manifest['environment'])
    env.pop('LD_PRELOAD', None)
    env.pop('LD_AUDIT', None)
    result = subprocess.run([str(nm), '-D', manifest['binary']['path']], env=env,
                            capture_output=True, text=True, start_new_session=True)
    assert result.returncode == 0, result.stderr
    for symbol in (b.EVALUATE_SYMBOL, 'r2f_fastdds_filter_trace_pop_v1', 'r2f_fastdds_filter_trace_stats_v1'):
        assert symbol + '@@CODEABI_1.0' in result.stdout


def test_probe_unsupported_platform_fails_before_loading(monkeypatch):
    monkeypatch.setattr(b.platform, 'machine', lambda: 'unsupported')
    with pytest.raises(ValueError, match='unsupported_native_platform'):
        b.no_dds_probe('/does-not-exist', '/does-not-exist-either')
