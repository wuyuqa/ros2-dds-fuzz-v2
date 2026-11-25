"""Build an isolated typed PDP interposer; never creates a DDS participant.

Run in a fresh process group. The peer vendor source/build and installation are
read-only. Both on/off selections use the exact same vendor and interposer DSO.
A successful ABI probe is compile evidence, never lifecycle execution evidence.
"""
from __future__ import annotations

import argparse
import ctypes as C
import json
import os
from pathlib import Path
import subprocess
import sys

from r2f.instrumentation.fastdds_peer.build import build_environment, tree_identity
from r2f.instrumentation.fastdds_peer.probe import validate_manifest
from r2f.instrumentation.fastdds_filter_trace.build import trace_command
from .activation import ABI, SCHEMA, digest, need
from .observer import Record, Stats

SYMBOLS=(
 '_ZN8eprosima8fastrtps4rtps9PDPSimple26createParticipantProxyDataERKNS1_20ParticipantProxyDataERKNS1_6GUID_tE',
 '_ZN8eprosima8fastrtps4rtps9PDPSimple21assignRemoteEndpointsEPNS1_20ParticipantProxyDataE',
 '_ZN8eprosima8fastrtps4rtps11PDPListener18process_alive_dataEPNS1_20ParticipantProxyDataERS3_RNS1_6GUID_tEPNS1_10RTPSReaderERSt11unique_lockISt15recursive_mutexE',
 '_ZN8eprosima8fastrtps4rtps3PDP25remove_remote_participantERKNS1_6GUID_tENS1_24ParticipantDiscoveryInfo16DISCOVERY_STATUSE',
 '_ZN8eprosima8fastrtps4rtps20ParticipantProxyData5clearEv',
 '_ZN8eprosima8fastrtps4rtps20ParticipantProxyData10updateDataERS2_',
 '_ZN8eprosima8fastrtps4rtps20ParticipantProxyDataC1ERKS2_',
 '_ZN8eprosima8fastrtps4rtps20ParticipantProxyDataC2ERKS2_')


def no_dds_probe(binary, vendor, refresh_binary=None):
    C.CDLL(str(vendor),mode=os.RTLD_GLOBAL|os.RTLD_NOW)
    library=C.CDLL(str(binary),mode=os.RTLD_LOCAL|os.RTLD_NOW)
    need(C.sizeof(Record)==96 and C.sizeof(Stats)==48,'probe_python_abi_mismatch')
    pop=library.r2f_fastdds_pdp_trace_pop_v1;pop.argtypes=[C.POINTER(Record),C.c_uint32];pop.restype=C.c_int32
    stats=library.r2f_fastdds_pdp_trace_stats_v1;stats.argtypes=[C.POINTER(Stats),C.c_uint32];stats.restype=C.c_int32
    need(pop(None,96)==-1 and pop(C.byref(Record()),95)==-1 and pop(C.byref(Record()),96)==0,'probe_pop_invalid')
    value=Stats();need(stats(C.byref(value),48)==0 and value.schema_version==1 and value.struct_size==48
                      and value.produced==value.pending==value.dropped==value.consumed==value.resolve_errors==0,
                      'probe_stats_invalid')
    if refresh_binary is not None:
        from .refresh import RefreshResult
        control=C.CDLL(str(refresh_binary),mode=os.RTLD_LOCAL|os.RTLD_NOW)
        function=control.r2f_fastdds_participant_refresh_writer_v1
        function.argtypes=[C.c_void_p,C.POINTER(C.c_uint8),C.c_uint32,C.POINTER(RefreshResult),C.c_uint32]
        function.restype=C.c_int32
        result=RefreshResult()
        need(C.sizeof(result)==576 and function(None,None,0,None,576)==-1
             and function(None,None,0,C.byref(result),575)==-1
             and function(None,None,0,C.byref(result),576)==1
             and result.schema_version==1 and result.struct_size==576 and result.complete==0,
             'refresh_control_no_dds_abi_invalid')
    return dict(status='passed',dds_entities_created=False,abi=ABI,
                refresh_control_abi='passed' if refresh_binary is not None else 'not_selected')


def run_build(peer_manifest, expected, root):
    root=Path(root).absolute();root.mkdir(exist_ok=False,parents=True)
    receipt=dict(schema=SCHEMA,status='building',observation_mode='instrumentation',abi=ABI,
                 verification={},commands=[],process=dict(pid=os.getpid(),pgid=os.getpgrp()))
    path=root/'build-manifest.json'
    def execute(name,command,env,cwd):
        command=list(map(str,command));log=root/(name+'.log')
        item=dict(name=name,command=command,cwd=str(cwd),log=str(log));receipt['commands'].append(item)
        with log.open('w') as stream:
            result=subprocess.run(command,env=env,cwd=cwd,stdout=stream,stderr=subprocess.STDOUT)
        item['returncode']=result.returncode
        need(result.returncode==0,'build_step_failed:'+name)
    try:
        need(os.getpid()==os.getpgrp(),'dedicated_process_group_required')
        peer_path=Path(peer_manifest).resolve(strict=True);peer=validate_manifest(peer_path,expected)
        need(peer.get('filter_trace') is None and peer.get('pdp_trace') is None,'plain_peer_build_required')
        receipt['peer_manifest_reference']=dict(manifest_path=str(peer_path),manifest_sha256=expected)
        source=Path(peer['source']);build=Path(peer['build'])
        need(tree_identity(source)==peer['source_frozen'],'peer_source_changed')
        need(digest(build/'compile_commands.json')==peer['compile_commands_sha256'],'peer_compile_commands_changed')
        receipt.update(vendor_source_identity_sha256=peer['source_frozen']['sha256'],
                       compile_commands_sha256=peer['compile_commands_sha256'],vendor_binary=peer['binaries']['vendor'])
        hook=Path(__file__).with_name('hook.cpp');snapshot=root/'hook.cpp';snapshot.write_bytes(hook.read_bytes())
        receipt.update(hook_source_sha256=digest(snapshot),builder_source_sha256=digest(__file__))
        install=root/'install';(install/'lib').mkdir(parents=True);receipt['install']=str(install)
        output=install/'lib/libr2f_fastdds_pdp_trace.so';exports=root/'exports.map'
        exports.write_text('CODEABI_1.0 { global: '+'; '.join(SYMBOLS)+
                           '; r2f_fastdds_pdp_trace_pop_v1; r2f_fastdds_pdp_trace_stats_v1; local: *; };\n')
        command,reference=trace_command(build,peer['binaries']['vendor']['path'],snapshot,output,exports)
        receipt['vendor_reference_command']=reference
        compiler=Path(command[0]).resolve(strict=True)
        need(peer['toolchain'].get(str(compiler))==digest(compiler),'compiler_identity_mismatch')
        receipt['compiler']={'path':str(compiler),'sha256':digest(compiler)}
        env=build_environment(compiler.parents[2])
        env['LD_LIBRARY_PATH']=str(Path(peer['binaries']['vendor']['path']).parent)+os.pathsep+env['LD_LIBRARY_PATH']
        env['PYTHONPATH']=str(Path(__file__).resolve().parents[4])+os.pathsep+env.get('PYTHONPATH','')
        env['R2F_FASTDDS_PDP_TRACE_ENABLED']='1'
        receipt['environment']={k:env[k] for k in ('PATH','LD_LIBRARY_PATH','PYTHONPATH','R2F_FASTDDS_PDP_TRACE_ENABLED')}
        execute('trace-build',command,env,reference['directory']);receipt['verification']['compilation']='passed'
        receipt['binary']=dict(path=str(output),sha256=digest(output))
        # A separate opt-in normal-input carrier, never preloaded by the observer.
        refresh=Path(__file__).with_name('refresh.cpp');refresh_snapshot=root/'refresh.cpp'
        refresh_snapshot.write_bytes(refresh.read_bytes())
        refresh_output=install/'lib/libr2f_fastdds_participant_refresh.so'
        refresh_exports=root/'refresh-exports.map'
        refresh_exports.write_text('{ global: r2f_fastdds_participant_refresh_writer_v1; local: *; };\n')
        refresh_command,_=trace_command(build,peer['binaries']['vendor']['path'],refresh_snapshot,refresh_output,refresh_exports)
        execute('refresh-control-build',refresh_command,env,reference['directory'])
        receipt['refresh_control']=dict(schema='r2f.fastdds-participant-refresh-build/v1',
            source_sha256=digest(refresh_snapshot),binary=dict(path=str(refresh_output),sha256=digest(refresh_output)),
            abi_size=576,compilation='passed',scope='explicit_normal_public_userdata_input_control_not_passive_observer')

        execute('no-dds-abi',[sys.executable,'-m',__package__+'.build','--probe','--binary',output,
                              '--vendor',peer['binaries']['vendor']['path'],'--refresh-binary',refresh_output],env,root)
        receipt['no_dds_probe']=json.loads((root/'no-dds-abi.log').read_text())
        need(receipt['no_dds_probe'].get('status')=='passed','no_dds_probe_failed')
        receipt['verification']['no_dds_abi']='passed'
        need(validate_manifest(peer_path,expected)==peer and tree_identity(source)==peer['source_frozen'],
             'peer_identity_changed_during_build')
        need(digest(hook)==receipt['hook_source_sha256'] and digest(__file__)==receipt['builder_source_sha256']
             and digest(refresh)==receipt['refresh_control']['source_sha256'],
             'observer_source_changed_during_build')
        receipt.update(status='built',actual_interposition_verified=False)
        for mode in ('on','off'):
            trace=dict(receipt,capture_enabled=mode=='on');tracepath=root/('trace-'+mode+'.json')
            tracepath.write_text(json.dumps(trace,indent=2,sort_keys=True)+'\n')
            selected=dict(peer,pdp_trace=dict(manifest_path=str(tracepath),manifest_sha256=digest(tracepath)))
            (root/('peer-'+mode+'.json')).write_text(json.dumps(selected,indent=2,sort_keys=True)+'\n')
    except Exception as exc:
        receipt.update(status='failed',error=str(exc),error_type=type(exc).__name__);raise
    finally:path.write_text(json.dumps(receipt,indent=2,sort_keys=True)+'\n')
    return path


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--peer-manifest');parser.add_argument('--peer-manifest-sha256');parser.add_argument('--root')
    parser.add_argument('--probe',action='store_true');parser.add_argument('--binary');parser.add_argument('--vendor');parser.add_argument('--refresh-binary')
    args=parser.parse_args()
    if args.probe:result=no_dds_probe(args.binary,args.vendor,args.refresh_binary)
    else:result={'manifest_path':str(run_build(args.peer_manifest,args.peer_manifest_sha256,args.root))}
    print(json.dumps(result))

if __name__=='__main__':main()
