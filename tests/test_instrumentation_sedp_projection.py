"""Offline SEDP evidence contracts; fixtures never transmit or create DDS entities."""
import copy
from fractions import Fraction
import hashlib
import json
from pathlib import Path
import struct
import tempfile
import unittest

from r2f.instrumentation.sedp import (parse_sedp_capture, reconcile_discovery_projection,
                                     check_offered_requested)
from r2f.instrumentation.sedp_fields import (LEGACY, BASELINE_FIELDS, decode_duration,
    parse_parameter_list, endpoint_fields, semantic_duration)

PREFIX = '011000000000000000000001'
GUID = PREFIX + '00001003'
BASE = 1700000000000000000


def hashed(path, data):
    path.write_bytes(data)
    return {'path': str(path), 'sha256': hashlib.sha256(data).hexdigest()}


def jsonraw(path, value):
    return hashed(path, json.dumps(value, sort_keys=True).encode())


def param(pid, data, endian='<'):
    data += bytes((-len(data)) % 4)
    return struct.pack(endian + 'HH', pid, len(data)) + data


def cdr(value, endian='<'):
    data = value.encode() + b'\0'
    return struct.pack(endian + 'I', len(data)) + data


def plist(endian='<', *, extra=b'', sentinel=True):
    return (param(0x5a, bytes.fromhex(GUID), endian)
        + param(5, cdr('rt/test', endian), endian)
        + param(7, cdr('std_msgs::msg::dds_::String_', endian), endian)
        + param(0x1a, struct.pack(endian + 'iiI', 2, 0, 0), endian)
        + extra + (struct.pack(endian + 'HH', 1, 0) if sentinel else b''))


def fields(data, endian='<', **kw):
    ref = {'payload_pcap_offset': 40}
    pl = parse_parameter_list(data, 0, len(data), endian, raw_ref=ref)
    pl.update(endian=endian, raw_ref=ref)
    return endpoint_fields(pl, role='writer', vendor_id='0110', protocol_version=[2, 1], **kw)[0]


def packet(data, endian='<', *, dispose=False, sequence=1):
    flags = (1 if endian == '<' else 0) | 4
    inline = b''
    if dispose:
        flags |= 2
        inline = param(0x71, b'\0\0\0\3', endian) + param(0x70, bytes.fromhex(GUID), endian) + struct.pack(endian+'HH', 1, 0)
    body = (b'\0\0' + struct.pack(endian + 'H', 16) + bytes(4)
            + bytes.fromhex('000003c2') + struct.pack(endian + 'iI', 0, sequence)
            + inline + (b'\0\3\0\0' if endian == '<' else b'\0\2\0\0') + data)
    rtps = b'RTPS\x02\x01\x01\x10' + bytes.fromhex(PREFIX) + bytes([0x15, flags]) + struct.pack(endian + 'H', len(body)) + body
    udp = struct.pack('!HHHH', 53410, 53412, len(rtps)+8, 0) + rtps
    ip = bytes.fromhex('4500') + struct.pack('!H', len(udp)+20) + bytes(5) + b'\x11' + bytes(2) + b'\x7f\0\0\1'*2
    return bytes(12) + b'\x08\0' + ip + udp


def pcap(records, endian='<', nano=True):
    magic = (b'\x4d\x3c\xb2\xa1' if endian == '<' else b'\xa1\xb2\x3c\x4d') if nano else (b'\xd4\xc3\xb2\xa1' if endian == '<' else b'\xa1\xb2\xc3\xd4')
    output = magic + struct.pack(endian+'HHIIII', 2, 4, 0, 0, 65535, 1)
    for stamp, frame in records:
        sec, ns = divmod(stamp, 10**9)
        output += struct.pack(endian+'IIII', sec, ns if nano else ns//1000, len(frame), len(frame)) + frame
    return output


def fixture(root, *, records=None, begin_ns=BASE+110, return_ns=BASE+130):
    raw = hashed(root/'wire.pcap', pcap(records or [(BASE+120, packet(plist()))]))
    receipt = {'ready': {'unix_ns': BASE+100}, 'unix_ns': BASE+160, 'raw': raw,
               'thread_stopped': True, 'socket_closed': True, 'health': {'status':'ok', 'kernel_drop_status':'known_at_stop'},
               'counters': {'kernel_drops':0}, 'limit_reason':None, 'error':None}
    artifact = {**receipt, 'receipt_raw': jsonraw(root/'capture.json', receipt)}
    wire = parse_sedp_capture(artifact)
    graph_value = {'source':'ros_graph', 'captured_at_unix_ns': BASE+140, 'endpoints':[
        {'endpoint_guid':GUID, 'role':'writer', 'topic':'/test', 'type':'std_msgs/msg/String',
         'qos':{'reliability':'reliable', 'durability':'volatile', 'deadline_ns':2**63-1,
                'liveliness':'automatic', 'liveliness_lease_duration_ns':2**63-1}}]}
    graph = {**graph_value, 'raw':jsonraw(root/'graph.json', graph_value)}
    identity = {'generation':'generation-a', 'pid':77, 'process_start_ticks':456, 'role':'writer'}
    events = [{'event':'endpoint_create_begin', **identity, 'unix_ns':begin_ns, 'monotonic_ns':1000},
              {'event':'endpoint_created', **identity, 'endpoint_gid':GUID, 'unix_ns':return_ns, 'monotonic_ns':1020}]
    ref = hashed(root/'lifecycle.jsonl', b''.join(json.dumps(e).encode()+b'\n' for e in events))
    bindings = [{**identity, 'endpoint_guid':GUID, 'begin_raw_ref':{**ref, 'event_index':0},
                 'return_raw_ref':{**ref, 'event_index':1}}]
    return wire, graph, bindings, {'start_ns':BASE+100, 'end_ns':BASE+150}


def reconcile(parts):
    w, g, b, window = parts
    return reconcile_discovery_projection(w, g, endpoint_bindings=b, observation_window=window)


class SedpFieldContracts(unittest.TestCase):
    def test_defaults_require_complete_list_and_have_protocol_provenance(self):
        observed = fields(plist())
        for name in ('durability','deadline','liveliness.kind','liveliness.lease_duration'):
            self.assertEqual(observed[name]['provenance'], 'protocol_default')
            self.assertTrue(observed[name]['parameter_list_complete'])
        self.assertEqual(observed['deadline']['value']['infinite'], True)
        incomplete = fields(plist(sentinel=False))
        for name in ('deadline','durability'):
            self.assertEqual(incomplete[name]['status'], 'unknown')
        disabled = fields(plist(), field_policy={'protocol_defaults':False})
        self.assertEqual(disabled['deadline']['status'], 'unknown')

    def test_endpoint_default_reliability_is_role_specific(self):
        for role, expected in [('writer','RELIABLE'),('reader','BEST_EFFORT')]:
            data = struct.pack('<HH',1,0)
            pl = parse_parameter_list(data,0,len(data),'<',raw_ref={'payload_pcap_offset':0})
            pl.update(endian='<',raw_ref={})
            result,_ = endpoint_fields(pl,role=role,vendor_id='ffff',protocol_version=[2,5])
            self.assertEqual(result['reliability']['value'], expected)
            self.assertEqual(result['reliability']['provenance'],'protocol_default')

    def test_duplicate_or_invalid_parameter_never_defaults(self):
        for extra in [param(0x23,b'\0'*4), param(0x23,b'\0'*8)*2,
                      param(0x1d,struct.pack('<i',55))]:
            f = fields(plist(extra=extra))
            name = 'durability' if extra[:2]==b'\x1d\0' else 'deadline'
            self.assertEqual(f[name]['status'],'unknown')
            self.assertNotEqual(f[name].get('provenance'),'protocol_default')

    def test_history_depth_extension_is_only_supporting(self):
        extra = param(0x40,struct.pack('<ii',0,7))
        result=fields(plist(extra=extra))
        self.assertEqual(result['history']['status'],'not_applicable_by_endpoint_schema')
        self.assertEqual(result['depth']['status'],'not_applicable_by_endpoint_schema')
        self.assertNotIn('depth',BASELINE_FIELDS)

    def test_sentinel_length_ignored_but_sentinel_required(self):
        data=struct.pack('<HH',1,123)
        self.assertTrue(parse_parameter_list(data,0,len(data),'<',raw_ref={})['complete'])
        self.assertFalse(parse_parameter_list(b'',0,0,'<',raw_ref={})['complete'])

    def test_legacy_fraction_requires_binary_provenance(self):
        data=struct.pack('<iI',2,0x20000000)
        value=decode_duration(data,'<',vendor_id='0110',protocol_version=[2,1])
        self.assertEqual(value['status'],'unknown')
        value=decode_duration(struct.pack('<iI',2,0),'<',vendor_id='0110',protocol_version=[2,1])
        self.assertEqual(value['nanoseconds'],2*10**9)
        with tempfile.TemporaryDirectory() as tmp:
            for (vendor, protocol, version), profile in LEGACY.items():
                ref=jsonraw(Path(tmp)/vendor, {'loaded_libraries':[{'sha256':profile['library_sha256']}]})
                policy={'vendor_versions':{vendor:version}, 'vendor_provenance':{vendor:ref}}
                value=decode_duration(data,'<',vendor_id=vendor,protocol_version=protocol,field_policy=policy)
                self.assertEqual(value['nanoseconds'],2125000000)
                self.assertEqual(value['encoding'],'seconds_fraction_2pow32')
                ref['sha256']='0'*64
                self.assertEqual(decode_duration(data,'<',vendor_id=vendor,protocol_version=protocol,field_policy=policy)['status'],'unknown')

    def test_vendor_rounding_is_applied_before_policy_comparison(self):
        with tempfile.TemporaryDirectory() as tmp:
            for (vendor, protocol, version), profile in LEGACY.items():
                ref=jsonraw(Path(tmp)/vendor, {'sha256':profile['library_sha256']})
                value=decode_duration(struct.pack('<iI',1,3),'<',vendor_id=vendor,protocol_version=protocol,
                    field_policy={'vendor_versions':{vendor:version},'vendor_provenance':{vendor:ref}})
                expected=1000000001 if vendor=='0110' else 1000000000
                self.assertEqual(value['nanoseconds'],expected)
                self.assertEqual(semantic_duration(value),Fraction(expected,10**9))
                self.assertEqual(value['exact_seconds_numerator'],2**32+3)

    def test_standard_fraction_exact_rational_and_infinity(self):
        value=decode_duration(struct.pack('>iI',2,0x80000000),'>',vendor_id='ffff',protocol_version=[2,5])
        self.assertEqual(value['nanoseconds'],2500000000)
        tiny=decode_duration(struct.pack('<iI',0,1),'<',vendor_id='ffff',protocol_version=[2,4])
        self.assertIsNone(tiny['nanoseconds'])
        self.assertEqual(semantic_duration(tiny),Fraction(1,2**32))
        infinity=decode_duration(struct.pack('<iI',0x7fffffff,0xffffffff),'<',vendor_id='ffff',protocol_version=[2,5])
        self.assertTrue(infinity['infinite'])
        self.assertEqual(decode_duration(struct.pack('<iI',-1,0),'<',vendor_id='ffff',protocol_version=[2,5])['status'],'unknown')

    def test_endian_pcap_raw_offsets_and_tamper(self):
        with tempfile.TemporaryDirectory() as tmp:
            for e in ('<','>'):
                ref=hashed(Path(tmp)/('wire'+e),pcap([(BASE,packet(plist(e),e))],e))
                parsed=parse_sedp_capture(ref)
                event=parsed['events'][0]
                self.assertEqual(event['fields']['topic']['value'],'rt/test')
                self.assertEqual(event['fields']['endpoint_guid']['value'],GUID)
                parameter=event['parameter_list']['parameters'][0]
                offset=parameter['raw_ref']['pcap_value_offset']
                self.assertEqual(Path(ref['path']).read_bytes()[offset:offset+16],bytes.fromhex(GUID))
                Path(ref['path']).write_bytes(b'tamper')
                with self.assertRaisesRegex(ValueError,'sha256'):parse_sedp_capture(ref)


class SedpProjectionContracts(unittest.TestCase):
    def test_pre_return_announce_has_independent_generation_binding(self):
        with tempfile.TemporaryDirectory() as tmp:
            result=reconcile(fixture(Path(tmp)))
            self.assertEqual(result['comparison_status'],'consistent',result['unknown_reasons'])
            self.assertEqual(len(result['comparisons']),len(BASELINE_FIELDS))
            self.assertEqual(result['actual_match'],'not_evaluated')

    def test_runtime_graph_entry_and_extended_qos_canonical_keys(self):
        # This is the public EndpointInfo export shape from worker.graph.entry
        # plus extended_graph_qos, not an endpoint creation/config dictionary.
        from types import SimpleNamespace
        from r2f.runtime.worker import extended_graph_qos
        graph_qos = SimpleNamespace(history=SimpleNamespace(name='KEEP_LAST'),
            deadline=SimpleNamespace(nanoseconds=2000000000),
            liveliness=SimpleNamespace(name='AUTOMATIC'),
            liveliness_lease_duration=SimpleNamespace(nanoseconds=5000000000))
        exported = extended_graph_qos(graph_qos, True)
        self.assertEqual(exported['liveliness_lease_duration_ns'], 5000000000)
        self.assertEqual(exported['deadline_ns'], 2000000000)
        with tempfile.TemporaryDirectory() as tmp:
            extra = (param(0x23, struct.pack('<iI', 2, 0))
                     + param(0x1b, struct.pack('<iiI', 0, 5, 0)))
            parts = fixture(Path(tmp), records=[(BASE+120, packet(plist(extra=extra)))])
            graph = parts[1]
            graph['endpoints'] = [{'node_name':'r2f_writer', 'node_namespace':'/',
                'topic_type':'std_msgs/msg/String', 'endpoint_gid':GUID,
                'role':'writer', 'topic':'/test', 'qos':{
                    'reliability':'reliable', 'durability':'volatile', 'depth':7, **exported}}]
            graph['raw'] = jsonraw(Path(tmp)/'graph.json', {k:v for k,v in graph.items() if k!='raw'})
            result = reconcile(parts)
            self.assertEqual(result['comparison_status'], 'consistent', result['unknown_reasons'])
            comparisons = {row['field']:row for row in result['comparisons']}
            self.assertTrue(comparisons['liveliness.lease_duration']['equal'])
            self.assertTrue(comparisons['deadline']['equal'])
            self.assertEqual(result['history_depth'], 'not_applicable_by_endpoint_schema')

    def test_announce_before_call_or_return_after_graph_is_unknown(self):
        for kw in ({'begin_ns':BASE+125},{'return_ns':BASE+145}):
            with tempfile.TemporaryDirectory() as tmp:
                result=reconcile(fixture(Path(tmp),**kw))
                self.assertEqual(result['comparison_status'],'unknown')
                self.assertIn('endpoint_creation_chronology_unbound:'+GUID,result['unknown_reasons'])

    def test_forged_or_missing_generation_binding_is_unknown(self):
        for mode in ('no_begin','generation','pid','index','same_graph'):
            with tempfile.TemporaryDirectory() as tmp:
                parts=fixture(Path(tmp)); b=parts[2][0]
                if mode=='no_begin':b.pop('begin_raw_ref')
                if mode=='generation':b['generation']='forged'
                if mode=='pid':b['pid']=True
                if mode=='index':b['begin_raw_ref']['event_index']=True
                if mode=='same_graph':b['begin_raw_ref']=parts[1]['raw']
                result=reconcile(parts)
                self.assertEqual(result['comparison_status'],'unknown')
                self.assertIn('unverified_process_generation_binding:'+GUID,result['unknown_reasons'])

    def test_equality_mismatch_is_separate_from_compatibility(self):
        with tempfile.TemporaryDirectory() as tmp:
            parts=fixture(Path(tmp));g=parts[1]
            g['endpoints'][0]['qos']['durability']='transient_local'
            ref=jsonraw(Path(tmp)/'graph.json',{k:v for k,v in g.items() if k!='raw'});g['raw']=ref
            result=reconcile(parts)
            self.assertEqual(result['comparison_status'],'mismatch')
            self.assertEqual(result['differences'][0]['field'],'durability')
            self.assertEqual(result['candidate_admission'],'not_performed')

    def test_projection_or_raw_tamper_remains_unknown(self):
        for mode in ('wire','graph','raw'):
            with tempfile.TemporaryDirectory() as tmp:
                parts=fixture(Path(tmp))
                if mode=='wire':parts[0]['events'][0]['fields']['topic']['value']='rt/forged'
                if mode=='graph':parts[1]['endpoints'][0]['topic']='/forged'
                if mode=='raw':Path(parts[2][0]['return_raw_ref']['path']).write_text('{}')
                self.assertEqual(reconcile(parts)['comparison_status'],'unknown')

    def test_missing_capture_or_degraded_capture_is_unknown(self):
        for mode in ('missing','drop','truncated','limit'):
            with tempfile.TemporaryDirectory() as tmp:
                parts=fixture(Path(tmp)); artifact=parts[0]['capture_receipt']
                if mode=='missing':parts[0]['capture_receipt']=None
                else:
                    if mode=='drop':artifact['counters']['kernel_drops']=1
                    if mode=='truncated':artifact['counters']['truncated_frames']=1
                    if mode=='limit':artifact['limit_reason']='max_bytes'
                    artifact['receipt_raw']=jsonraw(Path(tmp)/'capture.json',{k:v for k,v in artifact.items() if k!='receipt_raw'})
                self.assertEqual(reconcile(parts)['comparison_status'],'unknown')

    def test_dispose_or_missing_finite_graph_does_not_claim_mismatch(self):
        with tempfile.TemporaryDirectory() as tmp:
            parts=fixture(Path(tmp),records=[(BASE+120,packet(plist())),(BASE+135,packet(plist(),dispose=True,sequence=2))])
            result=reconcile(parts)
            self.assertEqual(result['comparison_status'],'unknown')
            self.assertIn('no_live_wire_endpoint_at_graph_snapshot:'+GUID,result['unknown_reasons'])
        with tempfile.TemporaryDirectory() as tmp:
            parts=fixture(Path(tmp));g=parts[1];g['endpoints']=[]
            g['raw']=jsonraw(Path(tmp)/'graph.json',{k:v for k,v in g.items() if k!='raw'})
            self.assertEqual(reconcile(parts)['comparison_status'],'unknown')

    def test_malformed_graph_field_types_are_unknown(self):
        with tempfile.TemporaryDirectory() as tmp:
            parts=fixture(Path(tmp));g=parts[1];g['endpoints'][0]['topic']=17
            g['raw']=jsonraw(Path(tmp)/'graph.json',{k:v for k,v in g.items() if k!='raw'})
            self.assertEqual(reconcile(parts)['comparison_status'],'unknown')

    def test_offered_requested_ranks_durations_and_unknowns(self):
        def observed(**values):return {k:{'status':'observed','value':v} for k,v in values.items()}
        offered=observed(reliability='RELIABLE',durability='TRANSIENT_LOCAL',**{'liveliness.kind':'MANUAL_BY_TOPIC','deadline':{'nanoseconds':1250000000},'liveliness.lease_duration':{'infinite':True}})
        requested=observed(reliability='BEST_EFFORT',durability='VOLATILE',**{'liveliness.kind':'AUTOMATIC','deadline':{'nanoseconds':1500000000},'liveliness.lease_duration':{'infinite':True}})
        result=check_offered_requested(offered,requested)
        self.assertTrue(result['compatible']);self.assertEqual(result['actual_match'],'not_evaluated')
        self.assertFalse(check_offered_requested(requested,offered)['compatible'])
        requested.pop('deadline')
        self.assertIsNone(check_offered_requested(offered,requested)['compatible'])
        requested['durability']={'status':'observed','value':'PERSISTENT'}
        self.assertFalse(check_offered_requested(offered,requested)['compatible'])
