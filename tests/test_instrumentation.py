import hashlib
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
import xml.etree.ElementTree as ET

from r2f.instrumentation import (
    build_trace_config, environment_for_mode, parse_trace, normalize_guid,
    capture_process_provenance, collect_trace, reconcile_projection,
)

# Offline fixture derived from the upstream 0.10.5 log format, not live evidence.
LINE = ('1794670000.123456 [174] dq.builtin: SEDP ST0 110abcd:22:33:402 '
        'reliable volatile writer unnamed: (default).rt/r2f_probe/std_msgs::msg::dds_::String_ '
        'NEW (as udp/127.0.0.1:50910@1) QOS={reliability=1:100000000,durability=0}\n')
GUID = '0110abcd000000220000003300000402'


class InstrumentationContracts(unittest.TestCase):
    def test_builder_preserves_transport_and_is_deterministic(self):
        with tempfile.TemporaryDirectory() as d:
            out = Path(d) / 'writer.log'
            base = '<CycloneDDS><Domain Id="174"><General><AllowMulticast>false</AllowMulticast></General><Tracing><Verbosity>finest</Verbosity></Tracing></Domain></CycloneDDS>'
            config = build_trace_config(out, domain_id=174, base_xml=base)
            root = ET.fromstring(config.xml)
            self.assertEqual(root.findtext('Domain/General/AllowMulticast'), 'false')
            self.assertEqual(len(root.findall('Domain/Tracing')), 1)
            self.assertEqual(root.findtext('Domain/Tracing/Category'), 'discovery,plist')
            self.assertEqual(config.sha256, build_trace_config(out, domain_id=174, base_xml=base).sha256)
            self.assertEqual(root.findtext('Domain/Tracing/AppendToFile'), 'false')
            self.assertEqual(config.environment['CYCLONEDDS_URI'], config.xml)
            out.touch()
            with self.assertRaises(FileExistsError):
                build_trace_config(out, domain_id=174)

    def test_builder_rejects_ambiguous_or_expanding_config(self):
        for path, domain, xml in [('/tmp/$HOME.log', 174, None), ('relative.log', 174, None),
                                  ('/tmp/trace.log', True, None),
                                  ('/tmp/trace.log', 174, '<CycloneDDS><Domain Id="173"/></CycloneDDS>'),
                                  ('/tmp/trace.log', 174, '<CycloneDDS><Domain/><Domain/></CycloneDDS>')]:
            with self.subTest(path=path, domain=domain), self.assertRaises(ValueError):
                build_trace_config(path, domain_id=domain, base_xml=xml)

    def test_blackbox_is_explicit_and_parent_environment_unchanged(self):
        env = {'CYCLONEDDS_URI': 'old.xml', 'R2F_INSTRUMENTATION': 'yes', 'LD_PRELOAD': '/probe.so',
               'LD_AUDIT': '/audit.so', 'PATH': '/opt/instrumentation/bin:/usr/bin',
               'LD_LIBRARY_PATH': '/opt/instrumentation/lib:/opt/ros/jazzy/lib', 'ROS_DOMAIN_ID': '174'}
        original = dict(env)
        clean = environment_for_mode(env, mode='blackbox', instrumentation_roots=('/opt/instrumentation',))
        self.assertNotIn('CYCLONEDDS_URI', clean)
        self.assertNotIn('LD_PRELOAD', clean)
        self.assertNotIn('LD_AUDIT', clean)
        self.assertNotIn('R2F_INSTRUMENTATION', clean)
        self.assertEqual(clean['PATH'], '/usr/bin')
        self.assertEqual(env, original)
        with self.assertRaises(ValueError):
            environment_for_mode(env, mode='instrumentation')
        with tempfile.TemporaryDirectory() as d:
            config = build_trace_config(Path(d) / 'x.log', domain_id=174)
            self.assertEqual(environment_for_mode(env, mode='instrumentation', trace_config=config)['CYCLONEDDS_URI'], config.xml)
            with self.assertRaises(ValueError):
                environment_for_mode(env, mode='blackbox', trace_config=config)

    def test_actual_source_format_and_byte_identity(self):
        parsed = parse_trace(LINE)
        event = parsed['events'][0]
        self.assertEqual(event['endpoint_guid'], GUID)
        self.assertEqual(event['topic_name'], 'rt/r2f_probe')
        self.assertEqual(event['type_name'], 'std_msgs::msg::dds_::String_')
        self.assertEqual(event['qos']['reliability'], 'reliable')
        self.assertEqual(event['qos']['durability'], 'volatile')
        self.assertIsNone(event['qos']['deadline'])
        self.assertIn('deadline', event['unknown_fields'])
        self.assertEqual(event['event_time_ns'], 1794670000123456000)
        self.assertEqual(event['timestamp_precision_ns'], 1000)
        self.assertEqual(event['raw_line_sha256'], hashlib.sha256(LINE.encode()).hexdigest())
        self.assertEqual(event['provenance_status'], 'unverified_input')
        self.assertEqual(event['instrumentation_point'], 'handle_sedp_alive_endpoint')

    def test_application_events_are_not_vendor_events(self):
        result = parse_trace('[INFO] [1794670000.123456] [node]: SEDP ST0 1:2:3:402\n' + json.dumps({'event': 'match', 'endpoint_guid': GUID}) + '\n')
        self.assertEqual(result['events'], [])

    def test_dispose_and_incomplete_are_retained(self):
        result = parse_trace('1794670000.000001 [174] dq.builtin: SEDP ST3 1:2:3:407 delete\n'
                             '1794670000.000002 [174] dq.builtin: SEDP ST0 1:2:3:402 unknown-proxypp\n')
        self.assertEqual(result['events'][0]['event'], 'sedp_endpoint_disposed')
        self.assertEqual(result['events'][0]['endpoint_kind'], 'reader')
        self.assertIsNone(result['events'][1]['topic_name'])
        self.assertEqual(len(result['diagnostics']), 1)

    def test_guid_padding(self):
        self.assertEqual(normalize_guid('110abcd:22:33:402'), GUID)
        self.assertEqual(normalize_guid(list(bytes.fromhex(GUID)) + [0] * 8), GUID)
        with self.assertRaises(ValueError):
            normalize_guid(bytes.fromhex(GUID) + b'\x01' * 8)
        with self.assertRaises(ValueError):
            normalize_guid('node_name')

    def test_missing_process_provenance_is_unknown(self):
        result = capture_process_provenance(999999999)
        self.assertFalse(result['vendor_loaded'])
        self.assertTrue(result['unknown'])

    def test_collect_does_not_accept_loader_probe_or_application_logs(self):
        with tempfile.TemporaryDirectory() as d:
            config = build_trace_config(Path(d) / 'vendor.log', domain_id=174)
            Path(config.output_file).write_text(LINE)
            result = collect_trace(config, processes=[{'scope': 'loader_probe_only', 'vendor_loaded': True}], control_task={'status': 'not_run'})
            self.assertFalse(result['internal_event_witness'])
            self.assertEqual(result['candidate_status'], 'none')
            self.assertEqual(result['events'][0]['provenance_status'], 'unknown')

    def _projection(self, d, *, missing=None):
        raw = Path(d) / 'vendor.log'
        raw.write_text(LINE)
        event = parse_trace(LINE)['events'][0]
        event['provenance_status'] = 'verified_vendor_process'
        trace = {'vendor_process_verified': True, 'raw_path': str(raw),
                 'raw_sha256': hashlib.sha256(raw.read_bytes()).hexdigest(), 'events': [event]}
        graph_path = Path(d) / 'graph.json'
        graph = {'source': 'ros_graph', 'raw_path': str(graph_path),
                 'raw_sha256': None,
                 'captured_at_unix_ns': 1794670000200000000,
                 'endpoints': [{'topic_name': '/r2f_probe', 'endpoint_guid': GUID, 'endpoint_kind': 'writer',
                                'qos': {'reliability': 'reliable', 'durability': 'volatile'}}]}
        graph_path.write_text(json.dumps({k: v for k, v in graph.items() if k not in ('raw_path', 'raw_sha256')}))
        graph['raw_sha256'] = hashlib.sha256(graph_path.read_bytes()).hexdigest()
        kwargs = {'dds_topic': 'rt/r2f_probe', 'ros_topic': '/r2f_probe', 'complete_window': True,
                  'window_start_ns': 1794670000000000000, 'window_end_ns': 1794670000300000000}
        return trace, graph, kwargs

    def test_projection_requires_two_sources_and_complete_fields(self):
        with tempfile.TemporaryDirectory() as d:
            trace, graph, kwargs = self._projection(d)
            result = reconcile_projection([trace], graph, **kwargs)
            self.assertEqual(result['verdict'], 'pass')
            self.assertEqual(result['target_maturity'], 'not_evaluated')
            kwargs['required_qos'] = ('reliability', 'durability', 'deadline')
            result = reconcile_projection([trace], graph, **kwargs)
            self.assertEqual(result['verdict'], 'unknown')
            self.assertTrue(any('deadline' in field for field in result['unknown_fields']))
            graph['source'] = 'vendor_internal_trace'
            self.assertEqual(reconcile_projection([trace], graph, **kwargs)['verdict'], 'unknown')

    def test_projection_reports_real_differences_and_raw_changes(self):
        with tempfile.TemporaryDirectory() as d:
            trace, graph, kwargs = self._projection(d)
            graph['endpoints'][0]['qos']['durability'] = 'transient_local'
            Path(graph['raw_path']).write_text(json.dumps({k: v for k, v in graph.items() if k not in ('raw_path', 'raw_sha256')}))
            graph['raw_sha256'] = hashlib.sha256(Path(graph['raw_path']).read_bytes()).hexdigest()
            self.assertEqual(reconcile_projection([trace], graph, **kwargs)['verdict'], 'fail')
            Path(trace['raw_path']).write_text('changed')
            result = reconcile_projection([trace], graph, **kwargs)
            self.assertIn('vendor_raw_hash_mismatch', result['unknown_fields'])


class FurtherInstrumentationContracts(unittest.TestCase):
    _projection = InstrumentationContracts._projection
    def test_actual_logged_qos_is_extracted_but_omissions_stay_unknown(self):
        line = LINE.replace('reliability=1:100000000,durability=0',
            'user_data=30<"deadline=999,history=0:999">,reliability=1:100000000,durability=0,'
            'deadline=9223372036854775807,liveliness=0:9223372036854775807,history=0:32,'
            'ownership=0,destination_order=0')
        qos = parse_trace(line)['events'][0]['qos']
        self.assertEqual(qos['depth'], 32)
        self.assertEqual(qos['deadline'], 9223372036854775807)
        self.assertEqual(qos['liveliness'], 'automatic')
        self.assertIsNone(qos['lifespan'])

    def test_read_atime_change_does_not_lose_vendor_evidence(self):
        from types import SimpleNamespace
        with tempfile.TemporaryDirectory() as d:
            config = build_trace_config(Path(d) / 'vendor.log', domain_id=174)
            Path(config.output_file).write_text(LINE)
            inode = Path(config.output_file).stat().st_ino
            values = dict(st_dev=1, st_ino=inode, st_size=len(LINE), st_mtime_ns=1, st_ctime_ns=1)
            before = SimpleNamespace(**values, st_atime_ns=1)
            after = SimpleNamespace(**values, st_atime_ns=2)
            with patch.object(Path, 'stat', side_effect=[before, after]):
                result = collect_trace(config, processes=[], control_task={'status': 'not_run'})
            self.assertEqual(len(result['events']), 1)
            after.st_mtime_ns = 2
            with patch.object(Path, 'stat', side_effect=[before, after]), self.assertRaises(ValueError):
                collect_trace(config, processes=[], control_task={'status': 'not_run'})

    def test_unbound_graph_projection_cannot_create_failure(self):
        with tempfile.TemporaryDirectory() as d:
            trace, graph, kwargs = self._projection(d)
            graph['endpoints'][0]['qos']['durability'] = 'transient_local'
            result = reconcile_projection([trace], graph, **kwargs)
            self.assertEqual(result['verdict'], 'unknown')
            self.assertIn('graph_projection_does_not_match_raw', result['unknown_fields'])

    def test_graph_self_comparison_is_unknown(self):
        with tempfile.TemporaryDirectory() as d:
            _, graph, kwargs = self._projection(d)
            result = reconcile_projection([graph], graph, **kwargs)
            self.assertEqual(result['verdict'], 'unknown')
            self.assertIn('vendor_process_provenance_unverified', result['unknown_fields'])


if __name__ == '__main__':
    unittest.main()
