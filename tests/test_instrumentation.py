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
        with tempfile.TemporaryDirectory() as directory:
            result = capture_process_provenance(999999999, proc_root=Path(directory))
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



class AdoptedConfigContracts(unittest.TestCase):
    def test_config_tracing_requires_explicit_opt_in(self):
        with tempfile.TemporaryDirectory() as directory:
            default = build_trace_config(Path(directory) / 'default.log', domain_id=174)
            config = build_trace_config(Path(directory) / 'adopted.log', domain_id=174,
                                        observe_adopted_config=True)
            self.assertEqual(default.categories, ('discovery', 'plist'))
            self.assertEqual(config.categories, ('discovery', 'plist', 'config'))
            self.assertEqual(ET.fromstring(config.xml).findtext('Domain/Tracing/Category'),
                             'discovery,plist,config')

    # Derived from pinned config.c cfg_note/print_configitems/pf_int64_unit,
    # NOT a captured vendor trace or parser acceptance sample.
    CONFIG_LINES = ('1794670000.123456 [174] main: config: Domain/General/Transport/#text: udp {0}\n'
                    '1794670000.123457 [174] main: config: Domain/Discovery/SPDPInterval/#text: 2 s {0,1}\n')

    def collect_offline(self, directory, raw, *, alter=None, enabled=True):
        config = build_trace_config(Path(directory) / 'adopted.log', domain_id=174,
                                    observe_adopted_config=enabled)
        path = Path(config.output_file)
        path.write_bytes(raw.encode() if isinstance(raw, str) else raw)
        stat = path.stat()
        proof = {'scope': 'observed_process', 'offline_fixture': True, 'pid': 910001,
                 'process_start_ticks': '9100010', 'process_identity_stable': True,
                 'vendor_loaded': True, 'config_matches': True, 'domain_matches': True,
                 'rmw_matches': True, 'trace_fd_attached': True, 'trace_fd': '7',
                 'trace_inode': stat.st_ino, 'trace_device': stat.st_dev,
                 'selected_environment': {'CYCLONEDDS_URI': config.xml,
                     'ROS_DOMAIN_ID': '174', 'RMW_IMPLEMENTATION': 'rmw_cyclonedds_cpp'},
                 'loaded_libraries': [{'mapped_path': '/offline/' + name, 'sha256': 'a' * 64,
                                       'identity_verified': True}
                                      for name in ('libddsc.so.0', 'librmw_cyclonedds_cpp.so')]}
        processes = [proof]
        if alter:
            alter(proof, processes)
        return collect_trace(config, processes=processes, control_task={'status': 'not_run'})

    def test_config_numeric_tokens_require_canonical_ascii_vendor_grammar(self):
        for token in ('1٢ s', '1２ s', '00 s', '01 s', '+1 s', '1e2 s'):
            with self.subTest(token=token), tempfile.TemporaryDirectory() as directory:
                settings = self.collect_offline(directory, self.CONFIG_LINES.replace('2 s', token))['adopted_domain_settings']
                field = settings['fields']['spdp_interval']
                self.assertEqual(field['status'], 'unknown')
                self.assertIsNone(field['value'])
                self.assertEqual(field['references'][0]['original_token'], token)
        for mask in ('{0,١}', '{0,01}', '{00}', '{٠}', '{0,1,02}'):
            with self.subTest(mask=mask), tempfile.TemporaryDirectory() as directory:
                result = self.collect_offline(directory, self.CONFIG_LINES.replace('{0,1}', mask))
                field = result['adopted_domain_settings']['fields']['spdp_interval']
                self.assertEqual(field['status'], 'unknown')
                self.assertIsNone(field['value'])
                self.assertEqual(len(field['references']), 1)
                self.assertIsNone(field['references'][0]['source_indices'])
                self.assertEqual(result['events'], [])
                self.assertEqual(result['candidate_status'], 'none')

    def test_configuration_projections_are_detached_json_values(self):
        with tempfile.TemporaryDirectory() as directory:
            original = self.collect_offline(directory, self.CONFIG_LINES)
            expected = json.loads(json.dumps(original['adopted_domain_settings']))
            original['adopted_domain_settings']['source_reference']['files']['offline_mutation'] = 'changed'
            with tempfile.TemporaryDirectory() as later_directory:
                later = self.collect_offline(later_directory, self.CONFIG_LINES)
            self.assertEqual(later['adopted_domain_settings']['source_reference'], expected['source_reference'])
            original['processes'][0]['loaded_libraries'][0]['sha256'] = 'changed'
            self.assertEqual(original['adopted_domain_settings']['binding'], expected['binding'])
            original['adopted_domain_settings']['binding']['loaded_libraries'][1]['sha256'] = 'changed-back'
            self.assertEqual(original['processes'][0]['loaded_libraries'][1]['sha256'], 'a' * 64)
            self.assertEqual(json.loads(json.dumps(later['adopted_domain_settings'])), later['adopted_domain_settings'])

    def test_selected_config_has_exact_refs_and_separate_sedp_events(self):
        with tempfile.TemporaryDirectory() as directory:
            raw = self.CONFIG_LINES + LINE
            result = self.collect_offline(directory, raw)
            settings = result['adopted_domain_settings']
            field = settings['fields']['spdp_interval']
            self.assertEqual(field['status'], 'observed')
            self.assertEqual(field['value'], 2000000000)
            self.assertEqual(field['unit'], 'ns')
            ref = field['references'][0]
            self.assertEqual(ref['line_number'], 2)
            self.assertEqual(ref['byte_offset'], len(self.CONFIG_LINES.splitlines(True)[0]))
            self.assertEqual(ref['byte_length'], len(self.CONFIG_LINES.splitlines(True)[1]))
            self.assertEqual(ref['original_token'], '2 s')
            self.assertEqual(ref['source_indices'], [0, 1])
            self.assertEqual(ref['raw_sha256'], hashlib.sha256(raw.encode()).hexdigest())
            chunk = raw.encode()[ref['byte_offset']:ref['byte_offset'] + ref['byte_length']]
            self.assertEqual(ref['raw_line_sha256'], hashlib.sha256(chunk).hexdigest())
            self.assertEqual(settings['fields']['transport']['value'], 'udp')
            self.assertEqual(len(result['events']), 1)
            self.assertEqual(settings['semantic_effect'], 'unknown')
            self.assertEqual(settings['parser_acceptance'], 'pending_real_trace')
            self.assertEqual(settings['binding']['process_start_ticks'], '9100010')
            self.assertEqual(result['candidate_status'], 'none')

    def test_source_derived_bad_and_partial_config_never_falls_back(self):
        cases = {
            'missing': ('', ('transport', 'spdp_interval')),
            'wrong_domain': (self.CONFIG_LINES.replace('[174]', '[175]'), ('transport', 'spdp_interval')),
            'duplicate_same': (self.CONFIG_LINES * 2, ('transport', 'spdp_interval')),
            'duplicate_conflict': (self.CONFIG_LINES + self.CONFIG_LINES.replace('2 s', '1 s'),
                                   ('transport', 'spdp_interval')),
            'truncated': (self.CONFIG_LINES.rstrip('\n'), ('transport', 'spdp_interval')),
            'duration_inf': (self.CONFIG_LINES.replace('2 s', 'inf'), ('spdp_interval',)),
            'duration_overflow': (self.CONFIG_LINES.replace('2 s', '9223372036854775808 ns'), ('spdp_interval',)),
            'duration_decimal': (self.CONFIG_LINES.replace('2 s', '1.5 s'), ('spdp_interval',)),
            'duration_negative': (self.CONFIG_LINES.replace('2 s', '-1 s'), ('spdp_interval',)),
            'duration_unit': (self.CONFIG_LINES.replace('2 s', '2 seconds'), ('spdp_interval',)),
            'duration_range': (self.CONFIG_LINES.replace('2 s', '3601 s'), ('spdp_interval',)),
            'bad_source_index': (self.CONFIG_LINES.replace('{0,1}', '{0,32}'), ('spdp_interval',)),
            'transport_unknown': (self.CONFIG_LINES.replace(': udp ', ': unicorn '), ('transport',)),
            'no_header': (self.CONFIG_LINES.replace('1794670000.123457 [174] main: ', ''), ('spdp_interval',)),
            'wrong_path': (self.CONFIG_LINES.replace('Domain/Discovery/', 'CycloneDDS/Domain/Discovery/'), ('spdp_interval',)),
            'multiple_threads': (self.CONFIG_LINES.replace('123457 [174] main:', '123457 [174] other:'),
                                 ('transport', 'spdp_interval')),
        }
        for label, (raw, unknown) in cases.items():
            with self.subTest(offline_fixture=label), tempfile.TemporaryDirectory() as directory:
                result = self.collect_offline(directory, raw)['adopted_domain_settings']
                for name, field in result['fields'].items():
                    self.assertEqual(field['status'], 'unknown' if name in unknown else 'observed')
                    if name in unknown:
                        self.assertIsNone(field['value'])

    def test_duration_units_use_logged_values_only_and_parse_is_bounded(self):
        for token, ns in (('1 ns', 1), ('2 us', 2000), ('3 ms', 3000000),
                          ('2 min', 120000000000), ('1 hr', 3600000000000), ('0 s', 0)):
            with self.subTest(token=token), tempfile.TemporaryDirectory() as directory:
                settings = self.collect_offline(directory, self.CONFIG_LINES.replace('2 s', token))['adopted_domain_settings']
                self.assertEqual(settings['fields']['spdp_interval']['value'], ns)
        for raw in (b'x' * (8 * 1024 * 1024 + 1), b'\n' * 32769,
                    self.CONFIG_LINES.replace('2 s', '2' * 4100 + ' s').encode()):
            with tempfile.TemporaryDirectory() as directory:
                fields = self.collect_offline(directory, raw)['adopted_domain_settings']['fields']
                self.assertTrue(all(f['status'] == 'unknown' for f in fields.values()))

    def test_config_process_binding_rejects_ambiguous_or_incomplete_proof(self):
        def extra(proof, processes):
            processes.append(dict(proof, pid=910002, config_matches=False))
        cases = [lambda p, ps: p.update(process_start_ticks=None),
                 lambda p, ps: p.update(trace_device=-1), lambda p, ps: p.update(trace_inode=-1),
                 lambda p, ps: p.update(scope='loader_probe_only'),
                 lambda p, ps: p.update(process_identity_stable=False),
                 lambda p, ps: p['selected_environment'].update(CYCLONEDDS_URI='<other/>'),
                 lambda p, ps: p['loaded_libraries'][0].update(identity_verified=False),
                 lambda p, ps: p['loaded_libraries'][0].update(sha256=None),
                 lambda p, ps: p.update(loaded_libraries=None),
                 lambda p, ps: p['loaded_libraries'][0].update(mapped_path=None), extra]
        for alter in cases:
            with self.subTest(offline_fixture=cases.index(alter)), tempfile.TemporaryDirectory() as directory:
                settings = self.collect_offline(directory, self.CONFIG_LINES, alter=alter)['adopted_domain_settings']
                self.assertIsNone(settings['binding'])
                self.assertTrue(all(f['status'] == 'unknown' and f['value'] is None
                                    for f in settings['fields'].values()))
        with tempfile.TemporaryDirectory() as directory:
            result = self.collect_offline(directory, self.CONFIG_LINES, enabled=False)
            self.assertNotIn('adopted_domain_settings', result)
            self.assertEqual(result['events'], [])


if __name__ == '__main__':
    unittest.main()
