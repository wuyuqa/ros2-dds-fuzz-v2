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


def with_drain(parts, root, cutoff=BASE+150):
    """Hash-bound new receipt with an empty witness and packets after that boundary."""
    artifact = parts[0]['capture_receipt']
    artifact.update(monotonic_ns=14000, complete_through_unix_ns=cutoff,
        drain={'status': 'drained', 'reason': 'socket_empty', 'nonblocking': True,
               'cutoff_unix_ns': cutoff, 'request': {'unix_ns': cutoff, 'monotonic_ns': 10000},
               'deadline_monotonic_ns': 20000,
               'empty_confirmed': {'unix_ns': BASE+152, 'monotonic_ns': 11000},
               'empty_statistics': {'packets': 1, 'drops': 0,
                                    'sampled': {'unix_ns': BASE+153, 'monotonic_ns': 12000}},
               'unread_packets_at_empty_boundary': 0,
               'ended': {'unix_ns': BASE+154, 'monotonic_ns': 13000},
               'post_empty_packets': 3, 'post_empty_drops': 0, 'kernel_unread_packets_at_stop': 3,
               'post_empty_scope': 'after_empty_statistics_outside_request_cutoff'})
    rebind_capture(parts, root)
    return parts


def rebind_capture(parts, root):
    artifact = parts[0]['capture_receipt']
    artifact['receipt_raw'] = jsonraw(root/'capture.json', {k:v for k,v in artifact.items() if k != 'receipt_raw'})


class SedpDrainCoverageContracts(unittest.TestCase):
    def test_legacy_and_new_window_inside_or_at_cutoff_remain_consistent(self):
        for cutoff in (None, BASE+150, BASE+151):
            with self.subTest(cutoff=cutoff), tempfile.TemporaryDirectory() as temp:
                root = Path(temp)
                parts = fixture(root)
                if cutoff is not None:
                    with_drain(parts, root, cutoff)
                self.assertEqual(reconcile(parts)['comparison_status'], 'consistent')

    def test_window_after_cutoff_but_before_close_is_unknown(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            parts = with_drain(fixture(root), root, BASE+145)
            self.assertLess(parts[3]['end_ns'], parts[0]['capture_receipt']['unix_ns'])
            result = reconcile(parts)
            self.assertEqual(result['comparison_status'], 'unknown')
            self.assertIn('capture_window_or_health_not_established', result['unknown_reasons'])

    def test_missing_incomplete_null_or_inconsistent_drain_cannot_claim_coverage(self):
        modes = ('incomplete', 'missing_complete', 'null_complete', 'bool_complete', 'float_complete',
                 'missing_drain', 'null_drain', 'cutoff_mismatch', 'request_mismatch', 'empty_before_cutoff',
                 'missing_empty', 'missing_statistics', 'statistics_before_empty', 'empty_after_deadline',
                 'unread_at_empty', 'blocking_receive', 'missing_clock', 'complete_after_close')
        for mode in modes:
            with self.subTest(mode=mode), tempfile.TemporaryDirectory() as temp:
                root = Path(temp)
                parts = with_drain(fixture(root), root)
                artifact = parts[0]['capture_receipt']
                drain = artifact['drain']
                if mode == 'incomplete': drain['status'] = 'incomplete'
                if mode == 'missing_complete': artifact.pop('complete_through_unix_ns')
                if mode == 'null_complete': artifact['complete_through_unix_ns'] = None
                if mode == 'bool_complete': artifact['complete_through_unix_ns'] = True
                if mode == 'float_complete': artifact['complete_through_unix_ns'] = float(BASE+150)
                if mode == 'missing_drain': artifact.pop('drain')
                if mode == 'null_drain': artifact['drain'] = None
                if mode == 'cutoff_mismatch': drain['cutoff_unix_ns'] += 1
                if mode == 'request_mismatch': drain['request']['unix_ns'] -= 1
                if mode == 'empty_before_cutoff': drain['empty_confirmed']['unix_ns'] = BASE+149
                if mode == 'missing_empty': drain.pop('empty_confirmed')
                if mode == 'missing_statistics': drain['empty_statistics'] = None
                if mode == 'statistics_before_empty': drain['empty_statistics']['sampled']['monotonic_ns'] = 10001
                if mode == 'empty_after_deadline': drain['deadline_monotonic_ns'] = 11000
                if mode == 'unread_at_empty': drain['unread_packets_at_empty_boundary'] = 1
                if mode == 'blocking_receive': drain['nonblocking'] = False
                if mode == 'missing_clock': drain['empty_confirmed'].pop('monotonic_ns')
                if mode == 'complete_after_close': artifact['complete_through_unix_ns'] = BASE+161
                rebind_capture(parts, root)
                self.assertEqual(reconcile(parts)['comparison_status'], 'unknown', mode)

    def test_unbound_cutoff_tamper_is_unknown(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            parts = with_drain(fixture(root), root)
            parts[0]['capture_receipt']['complete_through_unix_ns'] += 1
            self.assertEqual(reconcile(parts)['comparison_status'], 'unknown')


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


class IndependentSourceSchemaCP1(unittest.TestCase):
    """Simulated wire/graph inputs; offline contracts, never live evidence."""

    def test_layers_applicability_and_time_gap_are_explicit(self):
        with tempfile.TemporaryDirectory() as temp:
            result = reconcile(fixture(Path(temp)))
        schema = result['schema_check']
        self.assertEqual(schema['layers']['participant']['status'], 'unavailable_api')
        self.assertEqual(schema['layers']['topic']['status'], 'endpoint_derived_only')
        self.assertEqual(schema['layers']['endpoint']['required_fields'], list(BASELINE_FIELDS))
        self.assertEqual(schema['layers']['endpoint']['fields']['depth'],
                         'not_applicable_by_endpoint_schema')
        self.assertFalse(schema['all_layer_obligations_closed'])
        timing = schema['endpoint_observations'][0]
        self.assertIsNone(timing['graph_minus_wire_ns'])
        self.assertEqual(timing['timing_status'], 'unknown')
        self.assertEqual(timing['endpoint_guid'], GUID)
        self.assertEqual(timing['wire_unix_ns'], BASE+120)
        self.assertEqual(timing['graph_unix_ns'], BASE+140)
        self.assertIn('raw_ref', timing)

    def test_graph_label_cannot_be_used_as_independent_wire(self):
        with tempfile.TemporaryDirectory() as temp:
            parts = fixture(Path(temp))
            parts[0]['independent_source'] = 'ros_graph'
            result = reconcile(parts)
        self.assertEqual(result['comparison_status'], 'unknown')
        self.assertFalse(result['schema_check']['independent_sources_established'])

    def test_tampered_events_never_become_schema_observations(self):
        with tempfile.TemporaryDirectory() as temp:
            parts = fixture(Path(temp))
            parts[0]['events'][0]['unix_ns'] += 1
            result = reconcile(parts)
        self.assertFalse(result['schema_check']['independent_sources_established'])
        self.assertEqual(result['schema_check']['endpoint_observations'], [])


class SourceQualificationRepair(unittest.TestCase):
    """Simulated inputs; public annotation must retain evidence limitations."""

    def test_reported_wire_identity_must_be_the_validated_capture(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            parts = fixture(root)
            parts[0]['raw'] = jsonraw(root/'unrelated.json', {'unrelated': True})
            result = reconcile(parts)
        self.assertFalse(result['schema_check']['independent_sources_established'])
        self.assertEqual(result['schema_check']['source_qualification']['identity']['status'], 'unknown')
        self.assertEqual(result['schema_check']['endpoint_observations'], [])

    def test_structural_failures_do_not_establish_sources(self):
        for mode in ('container', 'row', 'truncated'):
            with self.subTest(mode=mode), tempfile.TemporaryDirectory() as temp:
                root = Path(temp)
                parts = fixture(root)
                if mode == 'truncated':
                    receipt = parts[0]['capture_receipt']
                    receipt['raw'] = hashed(root/'wire.pcap', pcap([(BASE+120, packet(plist()))])+b'X')
                    receipt['receipt_raw'] = jsonraw(root/'capture.json', {k:v for k,v in receipt.items() if k!='receipt_raw'})
                    parts = (parse_sedp_capture(receipt), *parts[1:])
                else:
                    parts[1]['endpoints'] = {} if mode == 'container' else [123]
                    parts[1]['raw'] = jsonraw(root/'graph.json', {k:v for k,v in parts[1].items() if k!='raw'})
                result = reconcile(parts)
                self.assertFalse(result['schema_check']['independent_sources_established'])
                self.assertEqual(result['schema_check']['source_qualification']['structure']['status'], 'unknown')
                self.assertEqual(result['schema_check']['endpoint_observations'], [])

    def test_malformed_ready_preserves_canonical_unknown_and_inputs(self):
        from r2f.instrumentation.sedp_projection import reconcile_discovery_projection as canonical
        for ready in (None, [], [1], 42, 'bad', False):
            with self.subTest(ready=ready), tempfile.TemporaryDirectory() as temp:
                root = Path(temp)
                parts = fixture(root)
                receipt = parts[0]['capture_receipt']
                receipt['ready'] = ready
                receipt['receipt_raw'] = jsonraw(root/'capture.json', {
                    k: v for k, v in receipt.items() if k != 'receipt_raw'})
                parts = (parse_sedp_capture(receipt), *parts[1:])
                before = copy.deepcopy(parts)
                expected = canonical(parts[0], parts[1], endpoint_bindings=parts[2],
                                     observation_window=parts[3])
                self.assertEqual(expected['comparison_status'], 'unknown')
                actual = reconcile(parts)
                schema = actual.pop('schema_check')
                self.assertEqual(actual, expected)
                self.assertEqual(parts, before)
                timing = schema['source_qualification']['timing']
                self.assertEqual(timing['status'], 'unknown')
                self.assertIn('capture_ready_mapping_required', timing['reasons'])
                self.assertIsNone(timing['wire_clock'])
                self.assertFalse(schema['all_layer_obligations_closed'])
                self.assertTrue(all(row['graph_minus_wire_ns'] is None
                                    for row in schema['endpoint_observations']))

    def test_receipt_times_require_exact_integer_nanoseconds(self):
        from r2f.instrumentation.sedp_projection import reconcile_discovery_projection as canonical
        for field, value in (('ready', True), ('ready', False),
                             ('ready', float(BASE+100)), ('close', float(BASE+160)),
                             ('close', True), ('close', False)):
            with self.subTest(field=field, value=value), tempfile.TemporaryDirectory() as temp:
                root = Path(temp)
                parts = fixture(root)
                receipt = parts[0]['capture_receipt']
                receipt['ready']['timestamp_clock'] = 'kernel_SO_TIMESTAMPNS_realtime'
                point = receipt['ready'] if field == 'ready' else receipt
                point['unix_ns'] = value
                rebind_capture(parts, root)
                parts[1]['timestamp_clock'] = 'unix_ns_realtime'
                parts[1]['raw'] = jsonraw(root/'graph.json',
                    {k: v for k, v in parts[1].items() if k != 'raw'})
                before = copy.deepcopy(parts)
                result = reconcile(parts)
                schema = result.pop('schema_check')
                self.assertEqual(result, canonical(parts[0], parts[1],
                    endpoint_bindings=parts[2], observation_window=parts[3]))
                self.assertEqual(parts, before)
                self.assertFalse(schema['all_layer_obligations_closed'])
                timing = schema['source_qualification']['timing']
                self.assertEqual(timing['status'], 'unknown')
                self.assertIn('capture_receipt_time_type_invalid', timing['reasons'])
                self.assertTrue(schema['endpoint_observations'])
                for row in schema['endpoint_observations']:
                    self.assertIsNone(row['graph_minus_wire_ns'])
                    self.assertEqual(row['timing_status'], 'unknown')
                    self.assertEqual(row['timing_reasons'], timing['reasons'])

    def test_drain_receipt_time_types_remain_required_for_qualification(self):
        from r2f.instrumentation.sedp_projection import reconcile_discovery_projection as canonical
        paths = [('complete_through_unix_ns',), ('monotonic_ns',),
                 ('drain', 'cutoff_unix_ns'), ('drain', 'deadline_monotonic_ns')]
        for point in (('drain', 'request'), ('drain', 'empty_confirmed'),
                      ('drain', 'empty_statistics', 'sampled'), ('drain', 'ended')):
            paths.extend(point + (clock,) for clock in ('unix_ns', 'monotonic_ns'))
        for path in (None, *paths):
            for kind in (('valid',) if path is None else ('bool_true', 'bool_false', 'float')):
                with self.subTest(path=path, kind=kind), tempfile.TemporaryDirectory() as temp:
                    root = Path(temp)
                    parts = with_drain(fixture(root), root)
                    receipt = parts[0]['capture_receipt']
                    receipt['ready']['timestamp_clock'] = 'kernel_SO_TIMESTAMPNS_realtime'
                    if path is not None:
                        point = receipt
                        for key in path[:-1]:
                            point = point[key]
                        point[path[-1]] = (float(point[path[-1]]) if kind == 'float'
                                          else kind == 'bool_true')
                    rebind_capture(parts, root)
                    parts[1]['timestamp_clock'] = 'unix_ns_realtime'
                    parts[1]['raw'] = jsonraw(root/'graph.json',
                        {k: v for k, v in parts[1].items() if k != 'raw'})
                    before = copy.deepcopy(parts)
                    result = reconcile(parts)
                    schema = result.pop('schema_check')
                    self.assertEqual(result, canonical(parts[0], parts[1],
                        endpoint_bindings=parts[2], observation_window=parts[3]))
                    self.assertEqual(parts, before)
                    self.assertFalse(schema['all_layer_obligations_closed'])
                    timing = schema['source_qualification']['timing']
                    self.assertEqual(timing['status'], 'qualified' if path is None else 'unknown')
                    if path is not None:
                        self.assertIn('observation_window_or_capture_coverage_not_established',
                                      timing['reasons'])
                    self.assertTrue(schema['endpoint_observations'])
                    for row in schema['endpoint_observations']:
                        self.assertEqual(row['graph_minus_wire_ns'], 20 if path is None else None)
                        self.assertEqual(row['timing_status'], timing['status'])
                        self.assertEqual(row['timing_reasons'], timing['reasons'])

    def test_clock_and_window_qualify_each_delta(self):
        for mode in ('missing', 'monotonic', 'graph_clock_missing', 'bad_window', 'valid', 'latest'):
            with self.subTest(mode=mode), tempfile.TemporaryDirectory() as temp:
                root = Path(temp)
                parts = fixture(root, records=[(BASE+120, packet(plist())),
                                               (BASE+135, packet(plist(), sequence=2))]) if mode == 'latest' else fixture(root)
                receipt = parts[0]['capture_receipt']
                if mode != 'missing':
                    receipt['ready']['timestamp_clock'] = ('monotonic_ns' if mode == 'monotonic'
                                                           else 'kernel_SO_TIMESTAMPNS_realtime')
                    receipt['receipt_raw'] = jsonraw(root/'capture.json', {k:v for k,v in receipt.items() if k!='receipt_raw'})
                if mode != 'graph_clock_missing':
                    parts[1]['timestamp_clock'] = 'unix_ns_realtime'
                parts[1]['raw'] = jsonraw(root/'graph.json', {k:v for k,v in parts[1].items() if k!='raw'})
                if mode == 'bad_window': parts[3]['end_ns'] = BASE+130
                result = reconcile(parts)
                row = result['schema_check']['endpoint_observations'][0]
                self.assertEqual(row['graph_minus_wire_ns'], (5 if mode == 'latest' else 20) if mode in ('valid', 'latest') else None)
                self.assertEqual(row['timing_status'], 'qualified' if mode in ('valid', 'latest') else 'unknown')
                self.assertEqual(row['timing_reasons'], result['schema_check']['source_qualification']['timing']['reasons'])

    def test_annotation_preserves_canonical_output_and_inputs(self):
        from r2f.instrumentation.sedp_projection import reconcile_discovery_projection as canonical
        with tempfile.TemporaryDirectory() as temp:
            parts = fixture(Path(temp))
            before = copy.deepcopy(parts)
            actual = reconcile(parts)
            expected = canonical(parts[0], parts[1], endpoint_bindings=parts[2], observation_window=parts[3])
        actual.pop('schema_check')
        self.assertEqual(actual, expected)
        self.assertEqual(parts, before)

    def test_unknown_qos_does_not_invalidate_source_identity(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            parts = fixture(root)
            parts[1]['endpoints'][0]['qos'].pop('reliability')
            parts[1]['raw'] = jsonraw(root/'graph.json', {k:v for k,v in parts[1].items() if k!='raw'})
            result = reconcile(parts)
        self.assertTrue(result['schema_check']['independent_sources_established'])
        self.assertEqual(result['comparison_status'], 'unknown')
