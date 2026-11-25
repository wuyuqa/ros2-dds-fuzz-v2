"""Authored observer manifests, never native execution evidence."""
from pathlib import Path
import json
from r2f.instrumentation.fastdds_pdp_trace import activation as pdp


def authored_pdp(base_manifest, root, *, enabled=True):
    base_path,base=base_manifest
    install=root/'trace-install';install.mkdir(exist_ok=True)
    binary=install/'libr2f_fastdds_pdp_trace.so';binary.write_bytes(b'authored-not-loadable')
    value=dict(schema=pdp.SCHEMA,status='built',observation_mode='instrumentation',abi=pdp.ABI,
        verification={'compilation':'passed'},capture_enabled=enabled,install=str(install),
        binary={'path':str(binary),'sha256':pdp.digest(binary)},vendor_binary=base['binaries']['vendor'],
        peer_manifest_reference={'manifest_path':str(base_path),'manifest_sha256':pdp.digest(base_path)},
        hook_source_sha256=pdp.digest(Path(pdp.__file__).with_name('hook.cpp')),
        builder_source_sha256=pdp.digest(Path(pdp.__file__).with_name('build.py')))
    trace=root/f'pdp-{enabled}.json';trace.write_text(json.dumps(value))
    selected=dict(base,pdp_trace={'manifest_path':str(trace),'manifest_sha256':pdp.digest(trace)})
    parent=root/f'peer-{enabled}.json';parent.write_text(json.dumps(selected))
    return parent,selected,value


def worker_rows(path, manifest):
    from test_fastdds_peer_activation import reference
    created=dict(event='endpoint_created',event_index=2,logical_id='w0',generation=1,
        endpoint_generation='g1',pid=100,pgid=100,process_start_ticks=10,
        run_id='offline-pdp',domain_id=196,observation_mode='instrumentation',candidate_eligible=False)
    selected=dict(created,event='native_peer_provider_selected',event_index=0,
        manifest_reference=reference(path),vendor_binary=manifest['binaries']['vendor'],
        provider_binary=manifest['binaries']['provider'],provider_abi=manifest['patch']['abi'],
        provider_source_sha256=manifest['provider_source_sha256'])
    return [selected,created]


def complete_rows(path,manifest,trace):
    import os
    rows=worker_rows(path,manifest);token=rows[-1]
    p=Path(trace['binary']['path']);st=p.stat()
    mapping=dict(path=str(p.resolve()),sha256=trace['binary']['sha256'],inode=st.st_ino,
                 device=f'{os.major(st.st_dev):02x}:{os.minor(st.st_dev):02x}')
    rows.insert(1,dict(token,event='native_pdp_provider_selected',event_index=1,
        manifest_reference=manifest['pdp_trace'],peer_manifest_reference=rows[0]['manifest_reference'],
        capture_enabled=trace['capture_enabled'],binary=trace['binary'],vendor_binary=trace['vendor_binary'],
        abi=trace['abi'],provider_mapping=mapping))
    if trace['capture_enabled']:
        for ordinal,phase in [(1,'entry'),(2,'return')]:
            receipt=dict(schema='r2f.fastdds-pdp-lifecycle-event/v1',observation_mode='instrumentation',
                ordinal=ordinal,capture_monotonic_ns=100+ordinal,call_id=1,parent_call_id=0,
                process_pid=100,thread_id=101,kind='proxy_clear',phase=phase,object_address=20,
                completion='entry' if phase=='entry' else 'returned',
                proxy_address=20,participant_guid='1'*32,pdp_lock_owned=None,
                result=-2 if phase=='entry' else 1,candidate_eligible=False,
                manifest_reference=manifest['pdp_trace'],provider_mapping=mapping)
            rows.append(dict(token,event='native_pdp_lifecycle',event_index=2+ordinal,monotonic_ns=110,
                             receipt=receipt))
    count=2 if trace['capture_enabled'] else 0
    rows.append(dict(token,event='worker_cleanup_step',event_index=5,monotonic_ns=200,component='context',status='returned'))
    rows.append(dict(token,event='worker_shutdown',event_index=6,monotonic_ns=210,cleanup_outcomes={'context':{'status':'returned'}}))
    rows.append(dict(token,event='native_pdp_trace_health',event_index=7,monotonic_ns=220,final=True,capture_complete=True,
        manifest_reference=manifest['pdp_trace'],stats=dict(schema_version=1,struct_size=48,produced=count,consumed=count,dropped=0,pending=0,
                   resolve_errors=0,enabled=int(trace['capture_enabled']))))
    return rows
