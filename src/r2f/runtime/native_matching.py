"""Explicit native DDS matching controls inside the canonical ROS worker.

Native entities have their own GUIDs, creation-time QoS, samples and cleanup.
They never stand in for adoption on the harness's RCL endpoints.
"""
from __future__ import annotations
import ctypes as c
from copy import deepcopy
import json
import os
import re
from pathlib import Path
import time
import uuid

from .matching_qos import digest, partitions, fastdds_partition_compatibility

SCHEMA = 'r2f.native-matching-control/v1'
TYPES = ('std_msgs/msg/String', 'std_msgs/msg/UInt8MultiArray')
ENUMS = {'reliability': ('best_effort', 'reliable'),
         'durability': ('volatile', 'transient_local', 'transient', 'persistent'),
         'liveliness': ('automatic', 'manual_by_participant', 'manual_by_topic'),
         'ownership': ('shared', 'exclusive'), 'history': ('keep_last', 'keep_all')}
INFINITE = 9223372036854775807


def ns(value):
    return INFINITE if value == 'infinite' else int(value * 1_000_000_000)


def validate_control(value):
    """Validate all requested semantics before accessing files or DDS."""
    from r2f.modules.s1 import topology_model as model
    required = {'support', 'topic', 'writer_qos', 'reader_qos', 'writer_type', 'reader_type',
                'same_participant', 'timeout_sec'}
    if not isinstance(value, dict) or set(value) != required:
        raise ValueError('native_matching_control_fields_invalid')
    ref = value['support']
    if (not isinstance(ref, dict) or set(ref) != {'path', 'sha256'}
            or not isinstance(ref['path'], str) or not Path(ref['path']).is_absolute()
            or not isinstance(ref['sha256'], str) or len(ref['sha256']) != 64
            or any(ch not in '0123456789abcdef' for ch in ref['sha256'])
            or type(value['same_participant']) is not bool
            or type(value['timeout_sec']) not in (int, float) or not 0.1 <= value['timeout_sec'] <= 15
            or not isinstance(value['topic'], str) or not 1 <= len(value['topic']) <= 128
            or not value['topic'].startswith('r2f_native_matching_')
            or any(ch not in 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_' for ch in value['topic'])
            or value['writer_type'] not in TYPES or value['reader_type'] not in TYPES):
        raise ValueError('native_matching_control_value_invalid')
    program = model.generate_program(20, writers=1, readers=1)
    program['actions'] = []
    for ep in program['endpoints']:
        qos = value[ep['role'] + '_qos']
        if not isinstance(qos, dict) or set(qos) != set(program['endpoints'][0]['qos']) or any(v is None for v in qos.values()):
            raise ValueError('native_matching_qos_complete_values_required')
        partitions(qos['partitions'])
        ep['qos'] = deepcopy(qos)
    if model.validate_program(program)['status'] != 'valid':
        raise ValueError('native_matching_qos_invalid')
    return deepcopy(value)


def compatibility(writer, reader, *, vendor):
    """Expected matching from independent native configuration readbacks."""
    reasons = []
    if writer['topic'] != reader['topic']: reasons.append('topic_mismatch')
    if writer['type_name'] != reader['type_name']: reasons.append('type_mismatch')
    w, r = writer['qos'], reader['qos']
    for key in ('reliability', 'durability', 'liveliness'):
        if ENUMS[key].index(w[key]) < ENUMS[key].index(r[key]): reasons.append(key + '_requested_offered')
    for key in ('deadline_sec', 'lease_duration_sec'):
        if ns(w[key]) > ns(r[key]): reasons.append(key + '_requested_offered')
    if w['ownership'] != r['ownership']: reasons.append('ownership_mismatch')
    if vendor == 'fastdds':
        matches = fastdds_partition_compatibility(w['partitions'], r['partitions'])
    else:
        # Cyclone ddsi_patmatch uses only '*' and '?'; bracket/backslash are literals.
        import re
        def match(pattern, literal):
            if any(ch in pattern for ch in '*?') and any(ch in literal for ch in '*?'):
                return False
            expression=b''.join(b'.*' if ch==42 else b'.' if ch==63 else re.escape(bytes([ch]))
                                for ch in pattern.encode('utf-8'))
            return re.fullmatch(expression, literal.encode('utf-8'), flags=re.DOTALL) is not None
        matches = any(match(a, b) or match(b, a) for a in w['partitions'] or [''] for b in r['partitions'] or [''])
    if not matches: reasons.append('partition_mismatch')
    return not reasons, reasons


def _function(lib, name, args, result):
    fn = getattr(lib, name); fn.argtypes, fn.restype = args, result
    return fn


def _cyclone(request, domain, observer):
    from .native_lookup_support import NativeLookupControl
    ref = request['support']
    support = NativeLookupControl(observer, {'manifest_path': ref['path'], 'manifest_sha256': ref['sha256'],
                                 'placement': 'same_participant'}, request['writer_type'])
    lib = observer.lib
    f = lambda name, args, ret=c.c_int32: _function(lib, name, args, ret)
    create_qos = f('dds_create_qos', [], c.c_void_p)
    delete_qos = f('dds_delete_qos', [c.c_void_p], None)
    create_pp = f('dds_create_participant', [c.c_uint32, c.c_void_p, c.c_void_p])
    create_topic = f('dds_create_topic', [c.c_int32, c.c_void_p, c.c_char_p, c.c_void_p, c.c_void_p])
    delete = f('dds_delete', [c.c_int32])
    owned, qos_objects = [], []
    receipt = {'vendor': 'cyclonedds', 'version': '0.10.5', 'api_provenance': observer.provenance,
               'support': deepcopy(ref), 'entities': [], 'samples': [], 'cleanup': [],
               'isolation': 'canonical_isolated_domain_unique_native_topic_owned_pair',
               'workload_path': 'native_dds_public_api', 'ros_qos_adoption': False}
    def entity(kind, handle):
        if handle <= 0: raise RuntimeError('native_create_' + kind + ':' + str(handle))
        owned.append((kind, handle)); return handle
    def qos_for(q):
        h = create_qos()
        if not h: raise RuntimeError('native_create_qos_failed')
        qos_objects.append(h)
        for key in ('reliability', 'durability', 'ownership', 'history', 'liveliness'):
            args, types = [h, ENUMS[key].index(q[key])], [c.c_void_p, c.c_int32]
            if key == 'reliability': args.append(100_000_000); types.append(c.c_int64)
            if key == 'history': args.append(q['depth']); types.append(c.c_int32)
            if key == 'liveliness': args.append(ns(q['lease_duration_sec'])); types.append(c.c_int64)
            f('dds_qset_' + key, types, None)(*args)
        f('dds_qset_deadline', [c.c_void_p, c.c_int64], None)(h, ns(q['deadline_sec']))
        names = (c.c_char_p * len(q['partitions']))(*(s.encode() for s in q['partitions']))
        f('dds_qset_partition', [c.c_void_p, c.c_uint32, c.POINTER(c.c_char_p)], None)(h, len(names), names)
        return h
    def read_qos(handle):
        h = create_qos(); qos_objects.append(h)
        if not h or f('dds_get_qos', [c.c_int32, c.c_void_p])(handle, h) != 0:
            raise RuntimeError('native_get_qos_failed')
        q = {}
        for key in ('reliability', 'durability', 'ownership', 'history', 'liveliness'):
            kind, extra = c.c_int32(), c.c_int64()
            args, types = [h, c.byref(kind)], [c.c_void_p, c.POINTER(c.c_int32)]
            if key in ('reliability', 'liveliness'): args.append(c.byref(extra)); types.append(c.POINTER(c.c_int64))
            depth = c.c_int32()
            if key == 'history': args.append(c.byref(depth)); types.append(c.POINTER(c.c_int32))
            if not f('dds_qget_' + key, types, c.c_bool)(*args): raise RuntimeError('native_qos_field_absent:' + key)
            q[key] = ENUMS[key][kind.value]
            if key == 'history': q['depth'] = depth.value
            if key == 'liveliness': q['lease_duration_sec'] = 'infinite' if extra.value == INFINITE else extra.value / 1e9
        deadline = c.c_int64()
        if not f('dds_qget_deadline', [c.c_void_p, c.POINTER(c.c_int64)], c.c_bool)(h, c.byref(deadline)):
            raise RuntimeError('native_deadline_absent')
        q['deadline_sec'] = 'infinite' if deadline.value == INFINITE else deadline.value / 1e9
        count, names = c.c_uint32(), c.POINTER(c.c_char_p)()
        if not f('dds_qget_partition', [c.c_void_p, c.POINTER(c.c_uint32), c.POINTER(c.POINTER(c.c_char_p))], c.c_bool)(h, c.byref(count), c.byref(names)):
            raise RuntimeError('native_partition_absent')
        if count.value > 64: raise RuntimeError('native_partition_readback_capacity')
        # dds_qget_partition allocates each string and its pointer array.
        addresses = c.cast(names, c.POINTER(c.c_void_p))
        free = f('dds_free', [c.c_void_p], None)
        q['partitions'] = []
        for i in range(count.value):
            q['partitions'].append(c.string_at(addresses[i]).decode()); free(addresses[i])
        free(c.cast(names, c.c_void_p))
        return q
    before_inventory=observer.sample_roots(domain)
    if before_inventory.get('complete') is not True:raise ValueError('native_control_initial_participant_inventory_incomplete')
    receipt['participant_inventory_before']=sorted(row['participant_gid'] for row in before_inventory['roots'])
    try:
        p1 = entity('participant', create_pp(domain, None, None))
        p2 = p1 if request['same_participant'] else entity('participant', create_pp(domain, None, None))
        for role, pp in (('writer', p1), ('reader', p2)):
            q = qos_for(request[role + '_qos'])
            topic = entity('topic', create_topic(pp, support.descriptor(int(request[role + '_type'] == TYPES[1])), request['topic'].encode(), None, None))
            parent_kind = 'publisher' if role == 'writer' else 'subscriber'
            parent = entity(parent_kind, f('dds_create_' + parent_kind, [c.c_int32, c.c_void_p, c.c_void_p])(pp, q, None))
            h = entity(role, f('dds_create_' + role, [c.c_int32, c.c_int32, c.c_void_p, c.c_void_p])(parent, topic, q, None))
            from .scoped_topic_lookup import _text_readback
            actual_domain = c.c_uint32()
            if f('dds_get_domainid', [c.c_int32, c.POINTER(c.c_uint32)])(h, c.byref(actual_domain)) != 0 or actual_domain.value != domain:
                raise RuntimeError('native_control_domain_readback_mismatch')
            receipt['entities'].append({'role': role, 'handle': h, 'generation': uuid.uuid4().hex,
                'endpoint_gid': observer._guid(h), 'participant_gid': observer._guid(pp), 'domain_id': actual_domain.value, 'qos': read_qos(h),
                'topic': _text_readback(c, lib.dds_get_name, topic),
                'type_name': _text_readback(c, lib.dds_get_type_name, topic), 'observed_monotonic_ns': time.monotonic_ns()})
        writer, reader = [row['handle'] for row in receipt['entities']]
        get_peers = [f('dds_get_matched_subscriptions', [c.c_int32, c.POINTER(c.c_uint64), c.c_size_t]),
                     f('dds_get_matched_publications', [c.c_int32, c.POINTER(c.c_uint64), c.c_size_t])]
        until = time.monotonic() + request['timeout_sec']
        while True:
            lists = []
            for fn, h, other in zip(get_peers, (writer, reader), reversed(receipt['entities'])):
                handles = (c.c_uint64 * 16)(); size = fn(h, handles, 16)
                if not 0 <= size <= 16: raise RuntimeError('native_matched_list_invalid:' + str(size))
                datafn = lib.dds_get_matched_subscription_data if h == writer else lib.dds_get_matched_publication_data
                datafn.argtypes, datafn.restype = [c.c_int32, c.c_uint64], c.POINTER(observer.Endpoint)
                gids=[]
                for ih in handles[:size]:
                    data=datafn(h, ih)
                    if not data: raise RuntimeError('native_peer_identity_unavailable')
                    gids.append(bytes(data.contents.key.v).hex())
                    f('dds_builtintopic_free_endpoint', [c.POINTER(observer.Endpoint)], None)(data)
                lists.append(gids)
            receipt['samples'].append({'monotonic_ns': time.monotonic_ns(), 'peer_gids': lists})
            if all(other['endpoint_gid'] in peers for peers, other in zip(lists, reversed(receipt['entities']))): break
            if time.monotonic() >= until: break
            time.sleep(0.05)
        expected, _ = compatibility(*receipt['entities'], vendor='cyclonedds')
        if expected and all(lists):
            # A compiled helper uses generated C types and dds_sample_info_t; no guessed sample ABI.
            text = ('native-matching-' + uuid.uuid4().hex).encode()
            write = _function(support.library, 'r2f_matching_write', [c.c_int32, c.c_char_p, c.c_int], c.c_int32)
            take = _function(support.library, 'r2f_matching_take', [c.c_int32, c.c_void_p, c.c_size_t, c.c_int, c.POINTER(c.c_uint64)], c.c_int32)
            rc = write(writer, text, int(request['writer_type'] == TYPES[1]))
            receipt['delivery'] = {'sent': text.decode(), 'write_return_code': rc, 'received': None}
            limit=time.monotonic()+request['timeout_sec']
            while rc == 0 and time.monotonic()<limit:
                output=c.create_string_buffer(1024); handle=c.c_uint64()
                got=take(reader, output, len(output), int(request['reader_type']==TYPES[1]), c.byref(handle))
                if got<0: raise RuntimeError('native_take_failed:'+str(got))
                if got:
                    receipt['delivery'].update(received=output.value.decode(), publication_handle=handle.value)
                    datafn=lib.dds_get_matched_publication_data
                    data=datafn(reader, handle.value)
                    if not data: raise RuntimeError('native_control_sample_publisher_unbound')
                    receipt['delivery']['sample_identity_writer_guid']=bytes(data.contents.key.v).hex()
                    lib.dds_builtintopic_free_endpoint(data)
                    break
                time.sleep(0.02)
        receipt['fresh_entities']=[]
        for original in receipt['entities']:
            receipt['fresh_entities'].append(dict(original, qos=read_qos(original['handle']),
                endpoint_gid=observer._guid(original['handle']), observed_monotonic_ns=time.monotonic_ns()))
        receipt['status']='observed'
    except (ValueError, RuntimeError, OSError, IndexError) as exc:
        receipt.update(status='unknown', reason=str(exc))
    finally:
        for kind,h in reversed(owned): receipt['cleanup'].append({'kind':kind,'handle':h,'return_code':delete(h)})
        for q in qos_objects:
            if q: delete_qos(q)
        after_inventory=observer.sample_roots(domain)
        receipt['participant_inventory_after']=sorted(row['participant_gid'] for row in after_inventory['roots'])
        receipt['cleanup_confirmed']=(all(row['return_code']==0 for row in receipt['cleanup']) and bool(owned)
            and after_inventory.get('complete') is True and receipt['participant_inventory_before']==receipt['participant_inventory_after'])
    return receipt


def execute_control(request, *, domain, rmw, endpoint, kind, output_dir=None):
    request=validate_control(request)
    start=time.monotonic_ns()
    if rmw == 'rmw_cyclonedds_cpp':
        from .topology_worker import CycloneMatchedPeerObserver
        value=_cyclone(request, domain, CycloneMatchedPeerObserver())
    elif rmw == 'rmw_fastrtps_cpp':
        value=_fastdds(request, domain, endpoint, kind, output_dir)
    else:
        value={'status':'unsupported','reason':'native_matching_vendor_not_implemented','cleanup_confirmed':True}
    value.update(schema=SCHEMA, request=deepcopy(request), started_monotonic_ns=start,
                 finished_monotonic_ns=time.monotonic_ns(), pid=os.getpid(), candidate_eligible=False,
                 workload_path='native_dds_public_api', ros_qos_adoption=False)
    return value


def _fastdds(request, domain, endpoint, kind, output_dir):
    from .matching_qos import load_build
    from .fastdds_peer_observer import verify_abi, _verify_api_maps
    verify_abi(); maps=_verify_api_maps()
    build=load_build(request['support'])
    if request['writer_type'] != TYPES[0] or request['reader_type'] != TYPES[0]:
        return {'status':'unsupported','reason':'native_fastdds_control_requires_String_harness',
                'cleanup_confirmed':True,'entities':[],'cleanup':[]}
    class Qos(c.Structure):
        _fields_=[(key,c.c_int32) for key in ('reliability','durability','liveliness','ownership','history','depth')]+[
            ('deadline',c.c_int64),('lease',c.c_int64),('partition_count',c.c_uint32),
            ('partitions',c.POINTER(c.c_char_p))]
    keep=[]
    def qos(q):
        names=(c.c_char_p*len(q['partitions']))(*(s.encode() for s in q['partitions']));keep.append(names)
        return Qos(*(ENUMS[key].index(q[key]) for key in ('reliability','durability','liveliness','ownership','history')),
                   q['depth'],ns(q['deadline_sec']),ns(q['lease_duration_sec']),len(names),names)
    persistence_dir=None
    if any(request[role+'_qos']['durability']=='transient' for role in ('writer','reader')):
        if output_dir is None or not Path(output_dir).is_absolute():
            raise ValueError('native_control_persistence_output_directory_required')
        persistence_dir=Path(output_dir)/uuid.uuid4().hex
        persistence_dir.mkdir(parents=True,exist_ok=False)
    database=str(persistence_dir/'native.db') if persistence_dir else ''
    w,r=qos(request['writer_qos']),qos(request['reader_qos'])
    library=c.CDLL(build['library'])
    layout=_function(library,'r2f_native_matching_qos_layout',[c.c_int],c.c_size_t)
    expected=[c.sizeof(Qos)]+[getattr(Qos,field).offset for field,_ in Qos._fields_]
    if [layout(i) for i in range(len(expected))]!=expected:raise ValueError('native_control_qos_abi_mismatch')
    fn=_function(library,'r2f_native_matching',[c.c_void_p,c.c_int,c.c_uint32,c.c_char_p,c.c_int,
        c.POINTER(Qos),c.POINTER(Qos),c.c_char_p,c.c_double,c.c_void_p,c.c_size_t],c.c_int)
    output=c.create_string_buffer(262144)
    with endpoint.handle as handle:
        # rclpy handle context returns the implementation, matching observe().
        pointer=endpoint.handle.pointer
        code=fn(pointer,int(kind=='reader'),domain,request['topic'].encode(),int(request['same_participant']),
                c.byref(w),c.byref(r),database.encode(),request['timeout_sec'],output,len(output))
    if code:raise ValueError('native_fastdds_control_bridge_error:'+str(code))
    value=json.loads(output.value)
    if persistence_dir is not None:
        value['persistence_artifacts']={'directory':str(persistence_dir),'creator_pid':os.getpid(),
            'recorded_monotonic_ns':time.monotonic_ns(),
            'files':[{'path':str(path),'sha256':digest(path),'size':path.stat().st_size}
                     for path in sorted(persistence_dir.iterdir()) if path.is_file()]}
    for row in value['entities']:
        row.update(generation=uuid.uuid4().hex)
    for fresh, original in zip(value.get('fresh_entities', []), value['entities']):
        fresh['generation']=original['generation']
    value.update(vendor='fastdds',version=build['fastdds_version'],support=deepcopy(request['support']),
                 api_provenance={'mapped_api_libraries':maps,'library':build['library'],
                     'library_sha256':build['files'][build['library']]},
                 isolation='canonical_isolated_domain_unique_native_topic_owned_pair')
    return value



def _audit_persistence(value, entities, fresh, pid, artifact_root, fixture):
    transient=[(initial,current) for initial,current in zip(entities,fresh)
               if initial['qos']['durability']=='transient']
    if value['vendor']!='fastdds' or not transient:
        return
    artifacts=value.get('persistence_artifacts')
    if not isinstance(artifacts,dict):raise ValueError('native_control_persistence_artifacts_missing')
    directory=Path(artifacts['directory'])
    if artifact_root is None:
        if not fixture:raise ValueError('native_control_persistence_trusted_worker_directory_missing')
    elif (directory.parent!=Path(artifact_root) or not re.fullmatch('[0-9a-f]{32}',directory.name)
            or directory.resolve(strict=True)!=directory
            or Path(artifact_root).resolve(strict=True)!=Path(artifact_root)):
        raise ValueError('native_control_persistence_directory_not_canonical_worker_owned')
    recorded=artifacts.get('recorded_monotonic_ns')
    if (not directory.is_absolute() or directory.is_symlink() or not directory.is_dir()
            or artifacts.get('creator_pid')!=pid or type(recorded) is not int
            or not max(e['observed_monotonic_ns'] for e in fresh)<=recorded<=value['finished_monotonic_ns']):
        raise ValueError('native_control_persistence_artifact_identity_invalid')
    rows=artifacts.get('files')
    if not isinstance(rows,list) or not rows:raise ValueError('native_control_persistence_artifacts_empty')
    paths=[]
    for row in rows:
        if not isinstance(row,dict):raise ValueError('native_control_persistence_artifact_not_object')
        path=Path(row['path'])
        if (path.parent!=directory or path.is_symlink() or not path.is_file()
                or digest(path)!=row.get('sha256') or path.stat().st_size!=row.get('size')):
            raise ValueError('native_control_persistence_artifact_changed')
        paths.append(str(path))
    if (len(paths)!=len(set(paths))
            or set(paths)!={str(path) for path in directory.iterdir()}):
        raise ValueError('native_control_persistence_artifact_inventory_changed')
    guids=[]
    for initial,current in transient:
        props=initial.get('persistence')
        if (not isinstance(props,dict) or current.get('persistence')!=props
                or props.get('dds.persistence.plugin')!='builtin.SQLITE3'
                or props.get('dds.persistence.sqlite3.filename') not in paths):
            raise ValueError('native_control_persistence_properties_missing_or_changed')
        guid=props.get('dds.persistence.guid')
        if (not isinstance(guid,str) or not re.fullmatch(r'(?:[0-9a-f]{2}\.){11}[0-9a-f]{2}\|(?:[0-9a-f]{2}\.){3}[0-9a-f]{2}',guid)):
            raise ValueError('native_control_persistence_guid_invalid')
        compact=guid.replace('.','').replace('|','')
        if compact[:24]!=initial['participant_gid'][:24] or compact in guids:
            raise ValueError('native_control_persistence_guid_not_owned_or_duplicate')
        guids.append(compact)


def audit_control(request, value, *, pid, fixture, artifact_root=None):
    """Audit a reopened raw result, with matching expected from actual QoS only."""
    validate_control(request)
    result={'status':'unknown','workload_path':'native_dds_public_api', 'ros_qos_adoption':False,
            's1_coverage_rows':['C10','C13','C15'],'cross_module_obligations':[
                'qos_policy','type_contract','transport_delivery','local_lifecycle'],
            'candidate_eligible':False,'required_denominator_credit':0}
    try:
        if (not isinstance(value,dict) or value.get('schema')!=SCHEMA or value.get('request')!=request
                or value.get('pid')!=pid or value.get('ros_qos_adoption') is not False
                or value.get('workload_path')!='native_dds_public_api'
                or type(value.get('started_monotonic_ns')) is not int
                or type(value.get('finished_monotonic_ns')) is not int
                or not 0<value['started_monotonic_ns']<=value['finished_monotonic_ns']
                or value.get('support')!=request['support'] or value.get('vendor') not in ('fastdds','cyclonedds')
                or value.get('cleanup_confirmed') is not True):
            raise ValueError('native_control_receipt_identity_or_cleanup_invalid')
        if not fixture:
            if not isinstance(value.get('api_provenance'),dict):raise ValueError('native_control_api_provenance_not_object')
            if value['vendor']=='fastdds':
                from .matching_qos import load_build
                build=load_build(request['support'])
                if (value.get('api_provenance') or {}).get('library_sha256')!=build['files'][build['library']]:
                    raise ValueError('native_control_api_build_mismatch')
            else:
                ref=request['support']; manifest=json.loads(Path(ref['path']).read_text())
                from .native_lookup_support import SOURCES,SCHEMA as SUPPORT_SCHEMA
                import hashlib
                from r2f.campaign.identity import source_manifest
                if (digest(ref['path'])!=ref['sha256'] or manifest.get('schema')!=SUPPORT_SCHEMA
                        or manifest.get('source')!=source_manifest(Path(__file__).resolve().parents[3])
                        or manifest.get('source_sha256')!={k:hashlib.sha256(v.encode()).hexdigest() for k,v in SOURCES.items()}
                        or digest(manifest['library_path'])!=manifest.get('library_sha256')
                        or any(digest(path)!=sha for path,sha in manifest['sdk_sha256'].items())):
                    raise ValueError('native_control_support_changed')
                provenance=value.get('api_provenance') or {}
                expected_library=Path('/opt/ros/jazzy/lib/x86_64-linux-gnu/libddsc.so.0.10.5').resolve()
                if (provenance.get('library')!=str(expected_library)
                        or provenance.get('library_sha256')!=digest(expected_library)
                        or provenance.get('header_fingerprints')!=manifest['sdk_sha256']):
                    raise ValueError('native_control_cyclone_api_provenance_invalid')
        cleanup=value.get('cleanup')
        before=value.get('participant_inventory_before')
        after=value.get('participant_inventory_after')
        if (not isinstance(before,list) or not before or before!=after
                or any(not isinstance(gid,str) or not re.fullmatch('[0-9a-f]{32}',gid)
                       or gid=='0'*32 for gid in before)
                or len(before)!=len(set(before))):
            raise ValueError('native_control_participant_cleanup_inventory_unproven')
        if not isinstance(cleanup,list) or not cleanup or any(not isinstance(row,dict) or type(row.get('return_code')) is not int or row['return_code']!=0 for row in cleanup):
            raise ValueError('native_control_cleanup_not_proven')
        if value.get('status')=='unsupported':
            # Fast DDS 2.14.7 check_qos explicitly rejects PERSISTENT, not a SUT failure.
            if (value['vendor']=='fastdds' and value.get('version')=='2.14.7'
                    and any(request[role+'_qos']['durability']=='persistent' for role in ('writer','reader'))
                    and value.get('reason') in ('native_control_create_datawriter_returned_null',
                                                'native_control_create_datareader_returned_null')):
                return dict(result,status='unsupported',reason=value['reason'],
                            applicability='pinned_vendor_rejects_persistent_durability',sut_failure=False)
            raise ValueError('native_control_unqualified_unsupported')
        if value.get('status')!='observed':raise ValueError(value.get('reason') or 'native_control_not_observed')
        entities=value.get('entities')
        if not isinstance(entities,list) or any(not isinstance(e,dict) for e in entities) or [e.get('role') for e in entities]!=['writer','reader']:
            raise ValueError('native_control_pair_inventory_incomplete')
        for e in entities:
            role=e['role']; q=e.get('qos'); wanted=request[role+'_qos']
            if (not isinstance(q,dict) or set(q)!=set(wanted) or e.get('topic')!=request['topic']
                    or not isinstance(e.get('generation'),str) or not e['generation']
                    or not re.fullmatch('[0-9a-f]{32}',e.get('endpoint_gid',''))
                    or not re.fullmatch('[0-9a-f]{32}',e.get('participant_gid',''))):
                raise ValueError('native_control_entity_binding_invalid')
            for key in wanted:
                if key=='depth' and wanted['history']=='keep_all':continue
                if q[key]!=wanted[key]:raise ValueError('native_control_qos_adoption_mismatch:'+role+':'+key)
            expected_type=('std_msgs::msg::dds_::String_' if value['vendor']=='fastdds' else
                           'r2f_lookup::'+('String' if request[role+'_type']==TYPES[0] else 'UInt8MultiArray'))
            if e.get('type_name')!=expected_type:raise ValueError('native_control_actual_type_mismatch')
        if entities[0]['endpoint_gid']==entities[1]['endpoint_gid'] or (
                (entities[0]['participant_gid']==entities[1]['participant_gid'])!=request['same_participant']):
            raise ValueError('native_control_guid_or_shared_owner_mismatch')
        fresh=value.get('fresh_entities')
        if not isinstance(fresh,list) or len(fresh)!=2 or any(not isinstance(e,dict) for e in fresh):
            raise ValueError('native_control_fresh_readback_missing')
        for initial,current in zip(entities,fresh):
            for key in ('role','endpoint_gid','participant_gid','qos','topic','type_name','generation','domain_id'):
                if current.get(key)!=initial.get(key):raise ValueError('native_control_fresh_readback_changed:'+key)
            if not (value['started_monotonic_ns']<=initial.get('observed_monotonic_ns',0)
                    <=current.get('observed_monotonic_ns',0)<=value['finished_monotonic_ns']):
                raise ValueError('native_control_readback_chronology_invalid')
        persistence_gaps=[]
        try:
            _audit_persistence(value,entities,fresh,pid,artifact_root,fixture)
        except (ValueError,KeyError,TypeError,OSError,IndexError,RuntimeError) as exc:
            persistence_gaps.append('persistence:'+str(exc))
        expected,reasons=compatibility(*entities,vendor=value['vendor'])
        samples=value.get('samples')
        if not isinstance(samples,list) or not samples:raise ValueError('native_control_match_samples_absent')
        flags_by_sample=[]; ever_by_sample=[]; times=[]; gaps=list(persistence_gaps); forbidden=[]
        for index,sample in enumerate(samples):
            flags_by_sample.append(None);ever_by_sample.append(None)
            try:
                if index>=400 or not isinstance(sample,dict):raise ValueError('native_control_match_sample_invalid')
                observed=sample.get('monotonic_ns')
                if (type(observed) is not int or (times and observed<=times[-1])
                        or observed<max(e['observed_monotonic_ns'] for e in entities)
                        or observed>min(e['observed_monotonic_ns'] for e in fresh)):
                    raise ValueError('native_control_match_sample_chronology_invalid')
                times.append(observed)
                if value['vendor']=='cyclonedds':
                    lists=sample.get('peer_gids')
                    if (not isinstance(lists,list) or len(lists)!=2 or any(not isinstance(peers,list) for peers in lists)):
                        raise ValueError('native_control_peer_lists_invalid')
                    flags=[other['endpoint_gid'] in peers for peers,other in zip(lists,reversed(entities))]
                    ever=flags
                    if any(len(peers)!=len(set(peers)) or set(peers)-{other['endpoint_gid']}
                           for peers,other in zip(lists,reversed(entities))):
                        raise ValueError('native_control_unexpected_peer')
                else:
                    counts=[sample.get('writer_count'),sample.get('reader_count')]
                    if any(type(n) is not int or n not in (0,1) for n in counts):raise ValueError('native_control_counts_invalid')
                    totals=[sample.get('writer_total_count'),sample.get('reader_total_count')]
                    last_gids=[sample.get('writer_last_peer_gid'),sample.get('reader_last_peer_gid')]
                    if any(type(n) is not int or n not in (0,1) for n in totals):
                        raise ValueError('native_control_matched_lifetime_identity_incomplete')
                    for count,total,gid,other in zip(counts,totals,last_gids,reversed(entities)):
                        if count>total or gid != (other['endpoint_gid'] if total else '0'*32):
                            raise ValueError('native_control_matched_counterpart_guid_missing_or_other_peer')
                    flags=[n==1 for n in counts]
                    ever=[n==1 for n in totals]
                flags_by_sample[index]=flags;ever_by_sample[index]=ever
                if not expected and any(ever):forbidden.append(index)
            except (ValueError,TypeError,KeyError) as exc:
                gaps.append('sample_'+str(index)+':'+str(exc))
        if forbidden:
            return dict(result,status='fail',reason='native_incompatible_pair_matched',entities=entities,
                        expected_compatible=expected,compatibility_reasons=reasons,
                        matched=flags_by_sample[forbidden[0]],ever_matched=ever_by_sample[forbidden[0]],
                        forbidden_match_sample_indexes=forbidden,observation_gaps=gaps)
        if gaps:raise ValueError(';'.join(gaps))
        if not expected and times[-1]-times[0] < max(0, request['timeout_sec']-0.1)*1e9:
            raise ValueError('native_control_refusal_observation_window_incomplete')
        flags=flags_by_sample[-1]
        if expected and flags!=[True,True]:raise ValueError('native_control_compatible_match_not_observed')
        if expected:
            delivery=value.get('delivery')
            if not isinstance(delivery,dict):raise ValueError('native_control_delivery_not_object')
            if (type(delivery.get('write_return_code')) is not int or delivery['write_return_code']!=0
                    or not isinstance(delivery.get('sent'),str) or not delivery['sent']
                    or delivery.get('received')!=delivery.get('sent')
                    or delivery.get('sample_identity_writer_guid')!=entities[0]['endpoint_gid']):
                raise ValueError('native_control_delivery_not_observed')
        return dict(result,status='pass',entities=entities,expected_compatible=expected,
                    compatibility_reasons=reasons,matched=flags,delivery=value.get('delivery'),
                    cleanup_confirmed=True,limitation='bounded_native_control_not_ROS_QoS_or_persistence_service_proof')
    except (ValueError,KeyError,TypeError,OSError,IndexError) as exc:
        return dict(result,reason=str(exc))


def audit_run(program, events, *, fixture, rmw, raw_path=None):
    """Bind each native action to one requested/result/ack worker chronology."""
    observations=[]
    actions=[a for a in program['actions'] if a['op']=='native_matching_control']
    for action in actions:
        row={'action_id':action['action_id'],'status':'unknown','candidate_eligible':False}
        try:
            eid,gen=action['endpoint_id'],action['generation']
            outputs=[e for e in events if e.get('event')=='native_matching_observation' and e.get('action_id')==action['action_id']]
            if len(outputs)!=1:raise ValueError('native_control_action_result_missing_or_duplicate')
            output=outputs[0];value=output['control_receipt']
            if not isinstance(value,dict):raise ValueError('native_control_raw_result_not_object')
            results=[e for e in events if e.get('event')=='native_matching_result'
                     and e.get('logical_id')==eid and e.get('generation')==gen and e.get('control_receipt')==value
                     and e['event_index']<output['event_index']]
            if len(results)!=1:raise ValueError('native_control_worker_result_missing_or_duplicate')
            result=results[0]
            keys=('logical_id','generation','endpoint_gid','endpoint_generation','pid','pgid','process_start_ticks')
            created=[e for e in events if e.get('event')=='endpoint_created' and all(e.get(k)==result.get(k) for k in keys)]
            requested=[e for e in events if e.get('event')=='native_matching_requested'
                       and e.get('command_id')==result.get('command_id') and all(e.get(k)==result.get(k) for k in keys)]
            acks=[e for e in events if e.get('event')=='command_done'
                  and e.get('command_id')==result.get('command_id') and all(e.get(k)==result.get(k) for k in keys)]
            if (len(created)!=1 or len(requested)!=1 or len(acks)!=1
                    or requested[0].get('control')!=action['params']['control']
                    or not created[0]['event_index']<requested[0]['event_index']<result['event_index']<acks[0]['event_index']<output['event_index']
                    or not requested[0]['monotonic_ns']<=value['started_monotonic_ns']<=value['finished_monotonic_ns']<=result['monotonic_ns']<=acks[0]['monotonic_ns']
                    or result.get('rmw_identifier')!=rmw):
                raise ValueError('native_control_request_ack_chronology_or_identity_invalid')
            entities=value.get('entities')
            if not isinstance(entities,list) or any(not isinstance(e,dict) for e in entities):
                raise ValueError('native_control_entities_not_object_array')
            if any(e.get('domain_id') != result.get('domain_id') for e in entities):
                raise ValueError('native_control_entity_domain_mismatch')
            artifact_root=(Path(raw_path).parent/'endpoints'/re.sub(r'[^A-Za-z0-9_.-]','_',eid)
                           /('g'+str(gen))/'native-controls') if raw_path else None
            row.update(audit_control(action['params']['control'],value,pid=result['pid'],fixture=fixture,
                                     artifact_root=artifact_root))
            row['raw_refs']=[e['event_index'] for e in (created[0],requested[0],result,acks[0],output)]
            row['vendor']=value.get('vendor');row['version']=value.get('version')
        except (ValueError,TypeError,KeyError) as exc:
            row['reason']=str(exc)
        observations.append(row)
    declared={a['action_id'] for a in actions}
    if any(e.get('event')=='native_matching_observation' and e.get('action_id') not in declared for e in events):
        observations.append({'status':'unknown','reason':'undeclared_native_matching_control','candidate_eligible':False})
    return observations
