"""Offline bridge contracts. Proc trees, library bytes and vendor lines are fixtures.

No DDS/ROS imports, participant, worker, campaign or live SUT evidence is created.
"""
from copy import deepcopy
import hashlib
import json
import os
from pathlib import Path
import shutil
import tempfile
import time
import unittest
from unittest.mock import patch
import xml.etree.ElementTree as ET

from r2f.core.candidate import CandidateLifecycle
from r2f.runtime import instrumentation_bridge as bridge

RMW = 'rmw_cyclonedds_cpp'
FASTDDS_RMW = 'rmw_fastrtps_cpp'
GUIDS = {'writer': '0110abcd000000220000003300000402',
         'reader': '0110abce000000220000003300000407'}
PROFILE = ('<CycloneDDS><Domain Id="any"><General><Transport>udp</Transport></General>'
           '<Discovery><ParticipantIndex>auto</ParticipantIndex></Discovery>'
           '<Tracing><Verbosity>finest</Verbosity></Tracing></Domain></CycloneDDS>')


def config(mode='instrumentation'):
    return {'run_id': 'offline-fixture', 'target_id': 'discovery.late_endpoint', 'target_revision': 0,
            'rmw': RMW, 'domain_id': 174, 'topic': '/r2f_probe', 'arm': 'baseline', 'repeat': 0,
            'seed': 23, 'observation_mode': mode, 'effective_input_sha256': 'a' * 64,
            'message_type': 'std_msgs/msg/String', 'input_layer_receipts': [],
            'final_input': {'payload': {'kind': 'normal'}, 'qos': {'reliability': 'reliable'}}}


class PairFixture:
    def __init__(self, root, mode='instrumentation', overrides=None, environment=None):
        self.root = Path(root)
        self.proc = self.root / 'proc'
        self.proc.mkdir(parents=True)
        self.patch = patch.object(bridge, '_PROC', self.proc)
        self.patch.start()
        self.c = config(mode)
        self.c.update(overrides or {})
        self.env = {'PATH': '/usr/bin', 'R2F_DISCOVERY_NETNS_PROOF': 'fixture-netns-proof'}
        if self.c['rmw'] == RMW:
            self.env['CYCLONEDDS_URI'] = PROFILE
        self.env.update(environment or {})
        self.ctx = bridge.prepare(self.c, self.root / 'run', self.env)
        self.events = []
        self.metadata = []
        self.libs = self.root / 'installed'
        self.libs.mkdir()
        names = (('libddsc.so.0', 'librmw_cyclonedds_cpp.so') if self.c['rmw'] == RMW else
                 ('libfastrtps.so.2.14', 'librmw_fastrtps_cpp.so', 'libfastcdr.so.2.2'))
        for name in names:
            (self.libs / name).write_bytes(('OFFLINE FIXTURE ' + name).encode())
        self.endpoints = {}
        for index, role in enumerate(('writer', 'reader')):
            pid = 910001 + index
            process = self.proc / str(pid)
            (process / 'fd').mkdir(parents=True)
            suffix = ['S', '1', str(pid)] + ['0'] * 16 + [str(pid * 10)]
            (process / 'stat').write_text(f'{pid} (offline worker) ' + ' '.join(suffix))
            (process / 'exe').symlink_to('/usr/bin/python3')
            (process / 'cwd').symlink_to(self.root.resolve())
            lines = []
            for index, lib in enumerate(sorted(self.libs.iterdir())):
                stat = lib.stat()
                # Typical ELF PT_LOAD mappings: repeated path/inode/device with
                # distinct address, offset and permissions, including RELRO.
                for segment, permissions in enumerate(('r--p', 'r-xp', 'r--p', 'r--p', 'rw-p')):
                    start = 0x7f0000000000 + index * 0x100000 + segment * 0x1000
                    lines.append(f'{start:x}-{start + 0x1000:x} {permissions} {segment * 0x1000:08x} '
                                 f'{os.major(stat.st_dev):02x}:{os.minor(stat.st_dev):02x} {stat.st_ino} {lib}\n')
            (process / 'maps').write_text(''.join(lines))
            self.write_env(role, pid)
            if mode == 'instrumentation':
                trace = Path(self.ctx.trace_configs[role].output_file)
                trace.touch()
                (process / 'fd' / '7').symlink_to(trace)
            event = {'event': 'endpoint_created', 'time_ns': time.time_ns(), 'monotonic_ns': time.monotonic_ns(),
                     'role': role, 'pid': pid, 'pgid': pid, 'run_id': self.c['run_id'], 'domain_id': self.c['domain_id'],
                     'rmw_identifier': self.c['rmw'], 'endpoint_generation': role + '-generation', 'node_name': 'r2f_' + role,
                     'endpoint_gid': GUIDS[role] + '0' * 16, 'topic': self.c['topic']}
            self.endpoints[role] = event
            self.events.append(event)
            self.metadata.append({'role': role, 'pid': pid, 'pgid': pid, 'returncode': 0})

    def write_env(self, role, pid, extra=None):
        env = dict(self.ctx.role_env[role], ROS_DOMAIN_ID=str(self.c['domain_id']), RMW_IMPLEMENTATION=self.c['rmw'])
        env.update(extra or {})
        (self.proc / str(pid) / 'environ').write_bytes(b'\0'.join((k + '=' + v).encode() for k, v in env.items()) + b'\0')

    def capture(self):
        for role, event in self.endpoints.items():
            self.ctx.capture_worker(role, event['pid'], event)

    def traces(self, missing=False):
        if self.ctx.mode != 'instrumentation':
            return
        now = time.time_ns()
        stamp = str(now // 10**9) + '.' + f'{now % 10**9:09d}'
        for observer, remote in (('writer', 'reader'), ('reader', 'writer')):
            words = ':'.join(GUIDS[remote][i:i+8] for i in range(0, 32, 8))
            line = (f'{stamp} [{self.c["domain_id"]}] dq.builtin: SEDP ST0 {words} reliable volatile {remote} unnamed: '
                    '(default).rt/r2f_probe/std_msgs::msg::dds_::String_ NEW (as udp/127.0.0.1:50910@1) '
                    'QOS={reliability=1:100000000,durability=0}\n')
            if missing:
                line = 'not a discovery record\n'
            Path(self.ctx.trace_configs[observer].output_file).write_text(line)

    def graph(self, role, *, durability='volatile', missing_qos=False, hook=True):
        event = deepcopy(self.endpoints[role])
        event.update(event='graph_snapshot', time_ns=time.time_ns(), monotonic_ns=time.monotonic_ns(),
                     command_id=1 if role == 'writer' else 2)
        for key, owner in (('publishers', 'writer'), ('subscriptions', 'reader')):
            qos = {'reliability': 'reliable', 'durability': durability}
            if missing_qos and owner == 'reader':
                qos.pop('reliability')
            event[key] = [{'endpoint_gid': GUIDS[owner] + '0' * 16, 'qos': qos,
                           'topic_type': 'std_msgs/msg/String', 'node_name': 'r2f_' + owner}]
        ack = deepcopy(self.endpoints[role])
        ack.update(event='command_done', time_ns=time.time_ns(), monotonic_ns=time.monotonic_ns(),
                   command_id=event['command_id'])
        match = dict(event, event='match_snapshot', endpoint_gid=self.endpoints[role]['endpoint_gid'],
                     sampled_monotonic_ns=event['monotonic_ns'] - 2, monotonic_ns=event['monotonic_ns'] - 1, matched_count=1)
        request = {'event': 'graph_snapshot_requested', 'role': role, 'run_id': self.c['run_id'],
                   'domain_id': self.c['domain_id'], 'command_id': event['command_id'], 'requested_monotonic_ns': event['monotonic_ns'] - 3}
        self.events.extend((request, match, event, ack))
        if hook:
            self.ctx.capture_graph(event, ack)
        return event, ack

    def finish(self, stop=True):
        if stop:
            for event in self.endpoints.values():
                shutil.rmtree(self.proc / str(event['pid']))
        self.events.append({'event': 'runtime_end', 'run_id': self.c['run_id'], 'domain_id': self.c['domain_id'],
                            'runtime_status': 'completed', 'time_ns': time.time_ns()})
        raw = self.ctx.out / 'raw.jsonl'
        raw.write_text(''.join(json.dumps(event, sort_keys=True) + '\n' for event in self.events))
        digest = hashlib.sha256(raw.read_bytes()).hexdigest()
        execution = {k: self.c[k] for k in ('run_id', 'target_id', 'target_revision', 'rmw', 'domain_id', 'arm', 'repeat')}
        execution.update(real_execution=True, runtime_status='completed', workers=self.metadata,
                         raw_path=str(raw), raw_sha256=digest, source={'head': 'offline', 'runtime_source_sha256': 'b' * 64},
                         source_after={'head': 'offline', 'runtime_source_sha256': 'b' * 64})
        return {'real_execution': True, 'runtime_status': 'completed', 'execution': execution,
                'raw_path': str(raw), 'raw_sha256': digest, 'events': deepcopy(self.events)}

    def ready(self, **graph_options):
        self.capture()
        self.traces()
        for role in ('writer', 'reader'):
            self.graph(role, **graph_options)
        return self.finish()

    def close(self):
        self.patch.stop()


class PreparationContracts(unittest.TestCase):
    def test_mode_is_execution_only_and_inputs_unchanged(self):
        with tempfile.TemporaryDirectory() as d:
            c = config()
            env = {'CYCLONEDDS_URI': PROFILE, 'PATH': '/usr/bin', 'LD_PRELOAD': 'trace.so',
                   'LD_AUDIT': 'audit.so', 'R2F_INSTRUMENT_FOO': 'yes', 'CYCLONEDDS_TRACE': 'yes',
                   'R2F_DISCOVERY_NETNS_PROOF': 'inherited'}
            before = deepcopy((c, env, dict(os.environ)))
            ctx = bridge.prepare(c, Path(d) / 'a', env)
            self.assertEqual((c, env, dict(os.environ)), before)
            self.assertEqual(ctx.role_env['writer']['R2F_DISCOVERY_NETNS_PROOF'], 'inherited')
            self.assertNotIn('LD_PRELOAD', ctx.base_env)
            self.assertNotIn('R2F_INSTRUMENT_FOO', ctx.role_env['writer'])
            c2 = dict(c, observation_mode='blackbox', domain_id=175)
            # Same semantic input object even with different execution mode/domain.
            blackbox = bridge.prepare(c2, Path(d) / 'b', env)
            self.assertEqual(c['final_input'], c2['final_input'])
            self.assertEqual(c['effective_input_sha256'], c2['effective_input_sha256'])
            self.assertEqual(ctx.profiles['writer']['non_tracing_sha256'], blackbox.profiles['writer']['non_tracing_sha256'])
            self.assertFalse(any(bridge._tag(e) == 'Tracing' for e in ET.fromstring(blackbox.role_env['writer']['CYCLONEDDS_URI']).iter()))
            self.assertEqual(ctx.role_env['writer']['CYCLONEDDS_URI'], ctx.trace_configs['writer'].xml)

    def test_both_modes_freeze_actual_execution_input_without_inventing_blackbox_task(self):
        for mode in ('instrumentation', 'blackbox'):
            with self.subTest(mode=mode), tempfile.TemporaryDirectory() as d:
                pair = PairFixture(d, mode=mode)
                self.addCleanup(pair.close)
                before = deepcopy(pair.c)
                summary = pair.ctx.collect(pair.ready())
                prepared = json.loads(Path(summary['prepared']['raw_path']).read_text())
                stored = json.loads(Path(summary['artifact']['raw_path']).read_text())
                actual = summary['execution_input']
                self.assertEqual(actual['schema'], 'r2f.observation-execution-input/v1')
                for key in ('run_id', 'domain_id', 'target_id', 'target_revision', 'rmw', 'arm',
                            'repeat', 'seed', 'topic', 'message_type', 'effective_input_sha256',
                            'final_input', 'input_layer_receipts'):
                    self.assertEqual(actual[key], before[key], key)
                self.assertEqual(prepared['execution_input'], actual)
                self.assertEqual(stored['execution_input'], actual)
                for ref in (summary['prepared'], summary['artifact']):
                    self.assertEqual(hashlib.sha256(Path(ref['raw_path']).read_bytes()).hexdigest(), ref['raw_sha256'])
                self.assertEqual(pair.c, before)
                if mode == 'blackbox':
                    self.assertIsNone(summary['blackbox_task'])
                    self.assertEqual(summary['blackbox_observer']['status'], 'verified')
                else:
                    self.assertEqual(summary['blackbox_task']['status'], 'not_run')
                # Returned summary mutations do not change the prepared or config snapshot.
                actual['final_input']['payload']['kind'] = 'changed'
                self.assertEqual(pair.c, before)
                self.assertEqual(prepared['execution_input']['final_input'], before['final_input'])

    def test_fastdds_default_blackbox_collects_vendor_libraries_and_explicit_applicability(self):
        with tempfile.TemporaryDirectory() as d:
            pair=PairFixture(d,mode='blackbox',overrides={'rmw':FASTDDS_RMW})
            self.addCleanup(pair.close)
            summary=pair.ctx.collect(pair.ready())
            self.assertEqual(summary['status'],'completed',summary)
            self.assertEqual(summary['blackbox_observer']['status'],'verified')
            self.assertEqual(summary['profile_policy']['cyclonedds_profiles'],'not_applicable')
            prepared=json.loads(Path(summary['prepared']['raw_path']).read_text())
            self.assertEqual(prepared['profile_policy'],summary['profile_policy'])
            self.assertEqual(prepared['profiles'],{})
            self.assertEqual(prepared['trace_configs'],{})
            self.assertIsNone(summary['blackbox_task'])
            for ref in summary['process_captures'].values():
                capture=json.loads(Path(ref['raw_path']).read_text())
                self.assertTrue(capture['accepted'],capture)
                self.assertTrue(capture['process']['vendor_loaded'])
                self.assertEqual(len(capture['process']['loaded_libraries']),3)
                self.assertTrue(all(lib['identity_verified'] for lib in capture['process']['loaded_libraries']))
                self.assertTrue(all(not record['exists'] for record in capture['audit']['default_profile_files'].values()))

    def test_fastdds_rejects_unsupported_profiles_and_unknown_modes_or_rmws(self):
        for key in ('CYCLONEDDS_URI','FASTDDS_DEFAULT_PROFILES_FILE','FASTRTPS_DEFAULT_PROFILES_FILE',
                    'FASTDDS_LOG_VERBOSITY','FASTRTPS_UNKNOWN_OPTION','RMW_FASTRTPS_USE_QOS_FROM_XML',
                    'SKIP_DEFAULT_XML','TRACETOOLS_TRACE_OUTPUT_DIRECTORY','ROS_TRACE_DIR'):
            with self.subTest(key=key),tempfile.TemporaryDirectory() as d:
                with self.assertRaisesRegex(ValueError,'unsupported FastDDS profile or trace environment'):
                    bridge.prepare(dict(config('blackbox'),rmw=FASTDDS_RMW),d,{key:''})
        for rmw,mode in ((FASTDDS_RMW,'instrumentation'),('unknown','blackbox'),(FASTDDS_RMW,'unknown')):
            with self.subTest(rmw=rmw,mode=mode),tempfile.TemporaryDirectory() as d:
                with self.assertRaises(ValueError):
                    bridge.prepare(dict(config(mode),rmw=rmw),d,{})

    def test_fastdds_live_process_contamination_and_missing_libraries_reject(self):
        cases=('env','trace','malformed_trace','loader','fd','library','maps','default_xml')
        for case in cases:
            with self.subTest(case=case),tempfile.TemporaryDirectory() as d:
                pair=PairFixture(d,mode='blackbox',overrides={'rmw':FASTDDS_RMW})
                self.addCleanup(pair.close)
                pid=pair.endpoints['writer']['pid']
                if case=='env':pair.write_env('writer',pid,{'FASTDDS_UNKNOWN_PROFILE':''})
                if case=='trace':pair.write_env('writer',pid,{'CYCLONEDDS_URI':PROFILE})
                if case=='malformed_trace':pair.write_env('writer',pid,{'CYCLONEDDS_URI':'<broken'})
                if case=='loader':pair.write_env('writer',pid,{'LD_AUDIT':'trace.so'})
                if case=='fd':
                    path=pair.ctx.directory/'foreign.trace.log';path.touch()
                    (pair.proc/str(pid)/'fd'/'8').symlink_to(path)
                if case in ('library','maps'):
                    maps=pair.proc/str(pid)/'maps'
                    if case=='library':
                        maps.write_text(''.join(line for line in maps.read_text().splitlines(True) if 'libfastcdr' not in line))
                    else:
                        fields=maps.read_text().splitlines()[0].split(maxsplit=5)
                        maps.write_text(maps.read_text().replace(' '+fields[4]+' ', ' 0 ',1))
                if case=='default_xml':(Path(d)/'DEFAULT_FASTRTPS_PROFILES.xml').write_text('<profiles/>')
                summary=pair.ctx.collect(pair.ready())
                capture=pair.ctx.workers['writer']
                self.assertFalse(capture['accepted'])
                if case in ('trace','malformed_trace'):
                    self.assertIn('unsupported_fastdds_profile_or_trace_environment',capture['audit']['errors'])
                self.assertEqual(summary['status'],'incomplete',summary)
                self.assertEqual(summary['blackbox_observer']['status'],'not_verified')
                self.assertEqual(summary['intake'],[])

    def test_multisegment_maps_accept_both_rmws_and_modes(self):
        for rmw,mode in ((RMW,'instrumentation'),(RMW,'blackbox'),(FASTDDS_RMW,'blackbox')):
            with self.subTest(rmw=rmw,mode=mode),tempfile.TemporaryDirectory() as d:
                pair=PairFixture(d,mode=mode,overrides={'rmw':rmw});self.addCleanup(pair.close)
                summary=pair.ctx.collect(pair.ready())
                self.assertEqual(summary['status'],'completed',summary)
                for capture in pair.ctx.workers.values():
                    self.assertTrue(capture['accepted'],capture)
                    for library in capture['process']['loaded_libraries']:
                        segments=[line.split(maxsplit=5) for line in capture['audit']['maps'].splitlines()
                                  if line.split(maxsplit=5)[5]==library['mapped_path']]
                        self.assertEqual(len(segments),5)
                        self.assertEqual({fields[1] for fields in segments},{'r--p','r-xp','rw-p'})

    def test_live_maps_reject_single_segment_conflicts_and_path_collisions(self):
        for rmw in (RMW,FASTDDS_RMW):
            for case in ('inode','device','absent','substring','nonexistent'):
                with self.subTest(rmw=rmw,case=case),tempfile.TemporaryDirectory() as d:
                    pair=PairFixture(d,mode='blackbox',overrides={'rmw':rmw});self.addCleanup(pair.close)
                    pid=pair.endpoints['writer']['pid'];maps=pair.proc/str(pid)/'maps'
                    fields=[line.split(maxsplit=5) for line in maps.read_text().splitlines()]
                    path=fields[0][5]
                    # Leave the first segment intact: collectors deduplicate
                    # library records, but every later segment still matters.
                    if case=='inode':fields[1][4]=str(int(fields[1][4])+1)
                    if case=='device':fields[1][3]='ff:ff'
                    if case=='absent':fields=[f for f in fields if f[5]!=path]
                    if case=='substring':
                        for f in fields:
                            if f[5]==path:f[5]+='.collision'
                    if case=='nonexistent':Path(path).unlink()
                    maps.write_text(''.join(' '.join(f)+'\n' for f in fields))
                    pair.capture()
                    capture=pair.ctx.workers['writer']
                    self.assertFalse(capture['accepted'],capture)
                    self.assertTrue(capture['audit']['errors'])

    def test_file_uri_and_role_transport_are_preserved(self):
        with tempfile.TemporaryDirectory() as d:
            path = Path(d) / 'transport.xml'
            path.write_text(PROFILE)
            role = {'writer': {'CYCLONEDDS_URI': path.as_uri(), 'ROLE_VALUE': 'writer'},
                    'reader': {'CYCLONEDDS_URI': PROFILE.replace('auto', '1')}}
            original = deepcopy(role)
            ctx = bridge.prepare(config('blackbox'), Path(d) / 'out', {'PATH': '/usr/bin'}, role)
            self.assertIn('<ParticipantIndex>auto</ParticipantIndex>', ctx.role_env['writer']['CYCLONEDDS_URI'])
            self.assertIn('<ParticipantIndex>1</ParticipantIndex>', ctx.role_env['reader']['CYCLONEDDS_URI'])
            self.assertEqual(ctx.role_env['writer']['ROLE_VALUE'], 'writer')
            self.assertEqual(ctx.profiles['writer']['source']['raw_sha256'], hashlib.sha256(path.read_bytes()).hexdigest())
            self.assertEqual(role, original)

    def test_trace_files_are_fresh_separate_and_absolute(self):
        with tempfile.TemporaryDirectory() as d:
            ctx = bridge.prepare(config(), d, {})
            paths = [Path(t.output_file) for t in ctx.trace_configs.values()]
            self.assertEqual(len(set(paths)), 2)
            self.assertTrue(all(p.is_absolute() and not p.exists() for p in paths))
            with self.assertRaises(FileExistsError):
                bridge.prepare(config(), d, {})

    def test_unknown_modes_domains_xml_and_rmw_fail_explicitly(self):
        cases = [dict(observation_mode='x'), dict(domain_id=True), dict(domain_id=232), dict(rmw='rmw_fastrtps_cpp')]
        with tempfile.TemporaryDirectory() as d:
            for i, extra in enumerate(cases):
                with self.subTest(extra=extra), self.assertRaises(ValueError):
                    bridge.prepare(dict(config(), **extra), Path(d) / str(i), {})
            for i, xml in enumerate(('<Bad/>', '<CycloneDDS><Domain/><Domain/></CycloneDDS>',
                                     '<CycloneDDS><Domain Id="42"/></CycloneDDS>')):
                with self.subTest(xml=xml), self.assertRaises(ValueError):
                    bridge.prepare(config(), Path(d) / ('xml' + str(i)), {'CYCLONEDDS_URI': xml})

    def test_default_blackbox_and_path_cleaning(self):
        with tempfile.TemporaryDirectory() as d:
            c = config()
            c.pop('observation_mode')
            c['instrumentation_roots'] = [str(Path(d) / 'instrumented')]
            env = {'LD_LIBRARY_PATH': str(Path(d) / 'instrumented/lib') + ':/opt/ros/jazzy/lib',
                   'LD_AUDIT': 'bad', 'CYCLONEDDS_URI': PROFILE}
            ctx = bridge.prepare(c, d, env)
            self.assertEqual(ctx.mode, 'blackbox')
            self.assertEqual(ctx.role_env['reader']['LD_LIBRARY_PATH'], '/opt/ros/jazzy/lib')
            self.assertNotIn('LD_AUDIT', ctx.role_env['writer'])


class ObservationContracts(unittest.TestCase):
    def fixture(self, root, *args, **kwargs):
        pair = PairFixture(root, *args, **kwargs)
        self.addCleanup(pair.close)
        return pair

    def test_complete_same_pair_trace_is_auditable_zero(self):
        with tempfile.TemporaryDirectory() as d:
            pair = self.fixture(d)
            result = pair.ready()
            before = deepcopy(result)
            raw_before = Path(result['raw_path']).read_bytes()
            summary = pair.ctx.collect(result)
            self.assertEqual(summary['status'], 'completed', summary['unknown_fields'])
            self.assertEqual(summary['verdict'], 'pass')
            self.assertTrue(summary['zero_candidate'])
            self.assertEqual(summary['intake'], [])
            self.assertEqual(summary['candidate_admission'], 'not_performed')
            self.assertTrue(all(t['vendor_process_verified'] for t in summary['traces']))
            self.assertEqual(len(summary['reconciliations']), 2)
            self.assertEqual(result, before)
            self.assertEqual(Path(result['raw_path']).read_bytes(), raw_before)
            artifact = summary['artifact']
            self.assertEqual(hashlib.sha256(Path(artifact['raw_path']).read_bytes()).hexdigest(), artifact['raw_sha256'])

    def test_failure_is_intake_for_existing_lifecycle(self):
        with tempfile.TemporaryDirectory() as d:
            pair = self.fixture(d)
            summary = pair.ctx.collect(pair.ready(durability='transient_local'))
            self.assertEqual(summary['verdict'], 'fail', summary['unknown_fields'])
            self.assertEqual(summary['candidate_count'], 1)
            evidence = summary['intake'][0]
            lifecycle = CandidateLifecycle('offline-observed-fixture', evidence)
            self.assertEqual(lifecycle.candidate_status, 'observed')
            with self.assertRaises(ValueError):
                lifecycle.admit()  # No invented stage/lifecycle admission.
            self.assertEqual(evidence['provenance']['control'], 'same-input blackbox task: not_run')

    def test_unknown_alongside_real_mismatch_never_enters_intake(self):
        with tempfile.TemporaryDirectory() as d:
            pair = self.fixture(d)
            summary = pair.ctx.collect(pair.ready(durability='transient_local', missing_qos=True))
            self.assertTrue(any(r['comparison']['mismatches'] for r in summary['reconciliations']))
            self.assertEqual(summary['verdict'], 'unknown')
            self.assertEqual(summary['intake'], [])
            self.assertFalse(summary['zero_candidate'])

    def test_missing_trace_is_not_successful_zero(self):
        with tempfile.TemporaryDirectory() as d:
            pair = self.fixture(d)
            result = pair.ready()
            Path(pair.ctx.trace_configs['writer'].output_file).unlink()
            summary = pair.ctx.collect(result)
            self.assertEqual(summary['status'], 'incomplete')
            self.assertFalse(summary['zero_candidate'])
            self.assertEqual(summary['candidate_count'], 0)

    def test_empty_trace_is_unknown(self):
        with tempfile.TemporaryDirectory() as d:
            pair = self.fixture(d)
            result = pair.ready()
            pair.traces(missing=True)
            summary = pair.ctx.collect(result)
            self.assertEqual(summary['verdict'], 'unknown')
            self.assertIn('empty_independent_projection', summary['unknown_fields'])

    def test_blackbox_verifies_disabled_trace_and_keeps_installed_vendor(self):
        with tempfile.TemporaryDirectory() as d:
            pair = self.fixture(d, 'blackbox')
            summary = pair.ctx.collect(pair.ready())
            self.assertEqual(summary['status'], 'completed', summary['unknown_fields'])
            self.assertEqual(summary['blackbox_observer']['status'], 'verified')
            self.assertEqual(summary['blackbox_validation'], 'not_applicable')
            self.assertEqual(summary['traces'], [])
            self.assertFalse(summary['zero_candidate'])
            self.assertTrue(pair.ctx.workers['writer']['process']['vendor_loaded'])

    def test_worker_pid_and_endpoint_token_must_match(self):
        with tempfile.TemporaryDirectory() as d:
            pair = self.fixture(d)
            event = pair.endpoints['writer']
            self.assertFalse(pair.ctx.capture_worker('writer', event['pid'] + 4, event)['accepted'])
            self.assertFalse(pair.ctx.capture_worker('reader', event['pid'], event)['accepted'])
            altered = dict(event, run_id='other')
            self.assertFalse(pair.ctx.capture_worker('writer', event['pid'], altered)['accepted'])

    def test_missing_maps_and_missing_trace_fd_remain_unknown(self):
        for missing in ('maps', 'fd/7'):
            with self.subTest(missing=missing), tempfile.TemporaryDirectory() as d:
                pair = self.fixture(d)
                event = pair.endpoints['writer']
                (pair.proc / str(event['pid']) / missing).unlink()
                summary = pair.ctx.collect(pair.ready())
                self.assertEqual(summary['verdict'], 'unknown')
                self.assertEqual(summary['candidate_count'], 0)

    def test_blackbox_trace_environment_or_fd_contamination_is_detected(self):
        for contamination in ('environment', 'fd', 'xml', 'namespace'):
            with self.subTest(contamination=contamination), tempfile.TemporaryDirectory() as d:
                pair = self.fixture(d, 'blackbox')
                pid = pair.endpoints['writer']['pid']
                if contamination == 'environment':
                    pair.write_env('writer', pid, {'LD_PRELOAD': '/tmp/trace.so'})
                elif contamination == 'fd':
                    trace = pair.ctx.directory / 'leaked.trace.log'
                    trace.touch()
                    (pair.proc / str(pid) / 'fd' / '8').symlink_to(trace)
                elif contamination == 'xml':
                    pair.write_env('writer', pid, {'CYCLONEDDS_URI': PROFILE})
                else:
                    pair.write_env('writer', pid, {'R2F_DISCOVERY_NETNS_PROOF': 'changed'})
                summary = pair.ctx.collect(pair.ready())
                self.assertEqual(summary['blackbox_observer']['status'], 'not_verified')
                self.assertEqual(summary['verdict'], 'unknown')

    def test_instrumentation_only_loaded_library_is_detected(self):
        with tempfile.TemporaryDirectory() as d:
            root = Path(d) / 'instrumented'
            pair = self.fixture(d, 'blackbox', {'instrumentation_roots': [str(root)]})
            pid = pair.endpoints['writer']['pid']
            maps = pair.proc / str(pid) / 'maps'
            maps.write_text(maps.read_text() + f'3000-4000 r-xp 0 00:00 1 {root}/hook.so\n')
            summary = pair.ctx.collect(pair.ready())
            self.assertIn('writer:instrumentation_library_loaded', summary['unknown_fields'])

    def test_graph_requires_command_bound_same_worker_unique_ack(self):
        for change in ('role', 'command', 'order', 'generation', 'automatic', 'duplicate'):
            with self.subTest(change=change), tempfile.TemporaryDirectory() as d:
                pair = self.fixture(d)
                pair.capture()
                graph, ack = pair.graph('writer', hook=False)
                if change == 'role':
                    ack['role'] = 'reader'
                elif change == 'command':
                    ack['command_id'] = 'other'
                elif change == 'order':
                    ack['monotonic_ns'] = graph['monotonic_ns'] - 1
                elif change == 'generation':
                    ack['endpoint_generation'] = 'other'
                elif change == 'automatic':
                    graph.pop('command_id')
                else:
                    self.assertTrue(pair.ctx.capture_graph(graph, ack)['accepted'])
                self.assertFalse(pair.ctx.capture_graph(graph, ack)['accepted'])

    def test_closed_raw_detects_duplicate_or_reordered_graph_ack(self):
        for change in ('duplicate', 'reorder', 'tamper'):
            with self.subTest(change=change), tempfile.TemporaryDirectory() as d:
                pair = self.fixture(d)
                pair.capture()
                pair.traces()
                graph, ack = pair.graph('writer')
                pair.graph('reader')
                if change == 'duplicate':
                    pair.events.append(deepcopy(ack))
                elif change == 'reorder':
                    i = pair.events.index(graph)
                    pair.events[i:i+2] = [ack, graph]
                else:
                    graph['publishers'][0]['qos']['durability'] = 'changed'
                summary = pair.ctx.collect(pair.finish())
                self.assertEqual(summary['verdict'], 'unknown')
                self.assertEqual(summary['intake'], [])

    def test_live_unreaped_workers_cannot_produce_closed_evidence(self):
        with tempfile.TemporaryDirectory() as d:
            pair = self.fixture(d)
            pair.capture()
            pair.traces()
            pair.graph('writer')
            pair.graph('reader')
            summary = pair.ctx.collect(pair.finish(stop=False))
            self.assertIn('original_worker_still_present:writer', summary['unknown_fields'])
            self.assertEqual(summary['intake'], [])

    def test_raw_hash_and_source_changes_suppress_intake(self):
        for change in ('raw', 'source', 'returncode', 'domain', 'capture'):
            with self.subTest(change=change), tempfile.TemporaryDirectory() as d:
                pair = self.fixture(d)
                result = pair.ready(durability='transient_local')
                if change == 'raw':
                    Path(result['raw_path']).write_text('changed\n')
                elif change == 'source':
                    result['execution']['source_after']['head'] = 'changed'
                elif change == 'returncode':
                    result['execution']['workers'][0]['returncode'] = 1
                elif change == 'domain':
                    result['execution']['domain_id'] = 175
                else:
                    Path(pair.ctx.workers['writer']['artifact']['raw_path']).write_text('{}')
                summary = pair.ctx.collect(result)
                self.assertEqual(summary['verdict'], 'unknown')
                self.assertEqual(summary['intake'], [])

    def test_raw_open_for_writing_is_not_closed(self):
        with tempfile.TemporaryDirectory() as d:
            pair = self.fixture(d)
            result = pair.ready()
            with Path(result['raw_path']).open('a'):
                summary = pair.ctx.collect(result)
            self.assertTrue(any('still open for writing' in e for e in summary['unknown_fields']))

    def test_trace_inode_replacement_is_unknown(self):
        with tempfile.TemporaryDirectory() as d:
            pair = self.fixture(d)
            result = pair.ready()
            path = Path(pair.ctx.trace_configs['writer'].output_file)
            replacement = path.with_suffix('.replacement')
            replacement.write_bytes(path.read_bytes())
            replacement.replace(path)
            summary = pair.ctx.collect(result)
            self.assertIn('trace_file_identity_changed:writer', summary['unknown_fields'])

    def test_foreign_guid_on_same_topic_does_not_become_sut_candidate(self):
        with tempfile.TemporaryDirectory() as d:
            pair = self.fixture(d)
            result = pair.ready()
            path = Path(pair.ctx.trace_configs['writer'].output_file)
            path.write_text(path.read_text().replace('0110abce:', '0110abcf:'))
            summary = pair.ctx.collect(result)
            self.assertIn('vendor_event_not_bound_to_pair', summary['unknown_fields'])
            self.assertEqual(summary['intake'], [])

    def test_controls_are_excluded_even_with_complete_internal_failure(self):
        for extra in ({'arm': 'negative'}, {'control_kind': 'fault'}, {'fault_id': 'omit'}):
            with self.subTest(extra=extra), tempfile.TemporaryDirectory() as d:
                pair = self.fixture(d, overrides=extra)
                summary = pair.ctx.collect(pair.ready(durability='transient_local'))
                self.assertEqual(summary['verdict'], 'fail', summary['unknown_fields'])
                self.assertTrue(summary['excluded_control'])
                self.assertEqual(summary['candidate_count'], 0)

    def test_blackbox_task_retains_identity_but_does_not_execute(self):
        with tempfile.TemporaryDirectory() as d:
            pair = self.fixture(d)
            summary = pair.ctx.collect(pair.ready())
            task = summary['blackbox_task']
            self.assertEqual(task['status'], 'not_run')
            self.assertEqual(task['same_input']['effective_input_sha256'], pair.c['effective_input_sha256'])
            self.assertEqual(task['same_input']['final_input'], pair.c['final_input'])
            self.assertNotIn('observation_mode', task['same_input'])
            self.assertNotIn('domain_id', task['same_input'])
            self.assertTrue(task['new_run_id_required'])
            self.assertFalse(task['executed_by_bridge'])


    def test_reused_pid_provenance_is_rejected(self):
        with tempfile.TemporaryDirectory() as d:
            pair = self.fixture(d)
            original = bridge.capture_process_provenance
            def reused(*args, **kwargs):
                proof = original(*args, **kwargs)
                proof['process_start_ticks'] = 'reused'
                return proof
            with patch.object(bridge, 'capture_process_provenance', side_effect=reused):
                pair.capture()
            pair.traces()
            pair.graph('writer')
            pair.graph('reader')
            summary = pair.ctx.collect(pair.finish())
            self.assertIn('writer:process_identity_changed_during_audit', summary['unknown_fields'])
            self.assertEqual(summary['intake'], [])

    def test_closed_raw_request_and_match_chain_is_required(self):
        for change in ('request', 'match', 'count', 'match_pid'):
            with self.subTest(change=change), tempfile.TemporaryDirectory() as d:
                pair = self.fixture(d)
                pair.capture()
                pair.traces()
                pair.graph('writer')
                pair.graph('reader')
                if change in ('request', 'match'):
                    name = 'graph_snapshot_requested' if change == 'request' else 'match_snapshot'
                    pair.events[:] = [e for e in pair.events if e.get('event') != name]
                else:
                    event = next(e for e in pair.events if e['event'] == 'match_snapshot')
                    event['matched_count' if change == 'count' else 'pid'] = 0
                summary = pair.ctx.collect(pair.finish())
                self.assertEqual(summary['verdict'], 'unknown')
                self.assertEqual(summary['intake'], [])

    def test_source_status_only_changes_do_not_change_behavior_identity(self):
        with tempfile.TemporaryDirectory() as d:
            pair = self.fixture(d)
            result = pair.ready()
            result['execution']['source']['worktree_status'] = 'before results/docs write'
            result['execution']['source_after']['worktree_status'] = 'after results/docs write'
            summary = pair.ctx.collect(result)
            self.assertEqual(summary['verdict'], 'pass', summary['unknown_fields'])

    def test_observer_source_change_is_unknown(self):
        with tempfile.TemporaryDirectory() as d:
            pair = self.fixture(d)
            result = pair.ready(durability='transient_local')
            with patch.object(bridge, '_observer_sources', return_value={'changed': 'c' * 64}):
                summary = pair.ctx.collect(result)
            self.assertIn('observer_source_changed', summary['unknown_fields'])
            self.assertEqual(summary['intake'], [])

    def test_mutated_intake_still_requires_real_mutation_gate(self):
        with tempfile.TemporaryDirectory() as d:
            pair = self.fixture(d, overrides={'arm': 'mutated'})
            summary = pair.ctx.collect(pair.ready(durability='transient_local'))
            self.assertIn('mutation_gate_incomplete', summary['unknown_fields'])
            self.assertEqual(summary['candidate_count'], 0)

    def test_failure_witness_precedes_its_graph_not_shutdown(self):
        with tempfile.TemporaryDirectory() as d:
            pair = self.fixture(d)
            result = pair.ready(durability='transient_local')
            path = Path(pair.ctx.trace_configs['writer'].output_file)
            later = time.time_ns() + 1000000000
            stamp = str(later // 10**9) + '.' + f'{later % 10**9:09d}'
            with path.open('a') as stream:
                stream.write(f'{stamp} [{pair.c["domain_id"]}] dq.builtin: SEDP ST3 0110abce:22:33:407 disposed\n')
            summary = pair.ctx.collect(result)
            self.assertEqual(summary['verdict'], 'fail', summary['unknown_fields'])
            witnesses = summary['intake'][0]['provenance']['witnesses']
            self.assertTrue(witnesses)
            for witness in witnesses:
                graph = json.loads(Path(witness['graph_raw_path']).read_text())
                self.assertLessEqual(witness['vendor_event']['event_time_ns'], graph['captured_at_unix_ns'])
                self.assertIn('raw_sha256', witness['vendor_event'])

    def test_collect_is_single_use_and_preserves_first_sidecar(self):
        with tempfile.TemporaryDirectory() as d:
            pair = self.fixture(d)
            result = pair.ready()
            summary = pair.ctx.collect(result)
            raw = Path(summary['artifact']['raw_path']).read_bytes()
            with self.assertRaises(RuntimeError):
                pair.ctx.collect(result)
            self.assertEqual(Path(summary['artifact']['raw_path']).read_bytes(), raw)


if __name__ == '__main__':
    unittest.main()
