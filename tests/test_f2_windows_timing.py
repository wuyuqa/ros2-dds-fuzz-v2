"""F2 fake-clock actuation only: no ROS, subprocess, campaign or netns."""
from copy import deepcopy
import hashlib
import json
from pathlib import Path
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from r2f.core.evidence import canonical_hash
from r2f.core.mutation_plan import MutationOperation, MutationPlan, MUTATION_PLAN_SCHEMA
from r2f.runtime import namespace_pair as ns
from r2f.runtime import ros2_pair as pair


def configuration(count=2, surface='E02', offsets=None, interval=0.3, anchor='fresh_match'):
    offsets = offsets or [0.1] * count
    windows = [dict(window_id=f'f2-{i}', window_index=i,
                    trigger_anchor='fresh_match' if i == 0 else 'previous_restore_confirmed',
                    trigger_offset_sec=offsets[i], drop_duration_sec=0.2,
                    restore_interval_sec=interval) for i in range(count)]
    final = dict(environment_windows=windows, runtime_capability='f2_windows_v1', anchor_timing_policy=pair.F2_ANCHOR_POLICY,
                 anchor_lateness_tolerance_sec=0.5)
    parameters = (dict(window_count=count, restore_interval_sec=interval,anchor='fresh_match',anchor_offset_sec=offsets[0]) if surface == 'E02'
                  else dict(anchor=anchor, offset_sec=offsets[0 if anchor=='fresh_match' else 1]))
    op = MutationOperation('offline-op', surface, 'M07' if surface == 'E02' else 'M06', parameters,
                           canonical_hash({}), canonical_hash(final), 'runtime_window',
                           {'status': 'needs_observation'})
    plan = MutationPlan(MUTATION_PLAN_SCHEMA, 'offline-deterministic', 1, canonical_hash({}),
                        [op], final, [dict(w, schema='r2f.runtime-window/v1') for w in windows], 'a' * 64)
    submitted = ({'environment_windows.count': count, 'environment_windows.restore_interval_sec': interval}
                 if surface == 'E02' else {'environment_windows.anchor': anchor, 'environment_windows.anchor_offset_sec': parameters['offset_sec']})
    c = pair.normalize_config(dict(target_id=pair.F2_WINDOWS_TARGET, target_revision=0,
        message_type='std_msgs/msg/String', run_id='offline-f2', domain_id=80, carrier='wire_proxy',
        mutation_kind='discovery_drop', fuzz_surfaces=[surface], submitted_parameters=submitted,
        mutation_plan=plan.to_dict(), mutation_plan_sha256=canonical_hash(plan.to_dict()),
        final_input=deepcopy(final), **final))
    return ns.normalize_discovery_config(c)


class Clock:
    def __init__(self):
        self.now = 1_000_000_000
    def monotonic_ns(self):
        return self.now
    def monotonic(self):
        return self.now / 1e9
    def time_ns(self):
        return 1_800_000_000_000_000_000 + self.now
    def advance(self, seconds):
        self.now += max(1, round(seconds * 1e9))


class FakeTrajectory:
    def __init__(self, events, endpoints, emit, start, deadline, matched):
        self.next_due_ns, self.after = deadline, None
    def poll(self, pool):
        pass


class Harness:
    """Portable event producer. All carrier commands are data, never executed."""
    def __init__(self, c):
        self.c, self.clock, self.events = c, Clock(), []
        self.pool = SimpleNamespace(pump=self.clock.advance)
        self.isolation = dict(isolated=True, interfaces=['lo'], net_namespace='net:[20]',
            user_namespace='user:[21]', observed_net_namespace='net:[20]', observed_user_namespace='user:[21]',
            host_net_namespace='net:[1]', host_user_namespace='user:[2]', host_uid=1000,
            uid_map=[[0, 1000, 1]], euid=0, cap_eff=1 << 12, host_cap_eff=0)
        self.emit('environment_preflight', ok=True)
        self.emit('namespace_host_admission_verified', isolation=self.isolation)
        self.emit('discovery_network_setup', receipt=dict(health_ok=True, drop_installed=False))
        self.endpoints = {}
        for i, role in enumerate(('writer', 'reader')):
            identity = dict(pid=110 + i, endpoint_generation=f'{role}.g1', rmw_identifier=c['rmw'])
            self.emit('process_started', role=role, **identity)
            self.endpoints[role] = self.emit('endpoint_created', role=role, endpoint_gid=f'{i+1:032x}',
                created_monotonic_ns=self.clock.now, topic=c['topic'], message_type=c['message_type'],
                qos=c['qos'], **identity)
        base = 7400 + 250 * c['domain_id']
        self.ports = {base, *(base + 10 + 2 * p for p in range(32))}
        self.bindings = dict(worker_pids={r: t['pid'] for r,t in self.endpoints.items()},
            worker_start_ticks=dict(writer='90', reader='91'), only_pair_bound_udp_sockets=True,
            isolation=self.isolation, bindings=[dict(port=base+10+2*i, roles=[r], selected_discovery_port=True)
                                               for i,r in enumerate(self.endpoints)])
        self.serial, self.installs, self.restores, self.tables = 0, [], [], []
        self.install_delay, self.zero_window = 0, None
        self.matched = self.snapshot_pair(self.clock.now)
        self.precondition = self.emit('discovery_precondition_snapshot', satisfied=True,
            matched=self.matched, bindings=self.bindings, worker_pids=self.bindings['worker_pids'],
            writer_gid=self.endpoints['writer']['endpoint_gid'], reader_gid=self.endpoints['reader']['endpoint_gid'])
        self.match_receipt = dict(domain_id=c['domain_id'], match_established=True,
            evidence_ref=self.precondition['raw_event_ref'], worker_pids=self.bindings['worker_pids'],
            **{r+'_gid': t['endpoint_gid'] for r,t in self.endpoints.items()})
        self.collections = [[] for _ in range(7)]

    def emit(self, event, **fields):
        token = dict(event=event, event_index=len(self.events), namespace_event_index=len(self.events),
            raw_event_ref=f'offline#event={len(self.events)}', role='controller', pid=100,
            run_id=self.c['run_id'], domain_id=self.c['domain_id'],
            monotonic_ns=self.clock.now, time_ns=self.clock.time_ns(), **{})
        token.update(deepcopy(fields))
        self.events.append(token)
        return token

    def snapshot_pair(self, anchor):
        matched, graphs = {}, {}
        for role, token in self.endpoints.items():
            self.serial += 1
            identity = {k: token[k] for k in ('pid', 'endpoint_generation', 'rmw_identifier')}
            self.emit('match_snapshot_requested', role=role, command_id=self.serial,
                requested_monotonic_ns=self.clock.now,
                reader_created_monotonic_ns=self.endpoints['reader']['created_monotonic_ns'])
            match = self.emit('match_snapshot', role=role, command_id=self.serial,
                sampled_monotonic_ns=self.clock.now, matched_count=1, endpoint_gid=token['endpoint_gid'],
                count_api='get_subscription_count' if role=='writer' else 'get_publisher_count', **identity)
            graph = self.emit('graph_snapshot', role=role, command_id=self.serial,
                publishers=[dict(endpoint_gid=self.endpoints['writer']['endpoint_gid'],qos=self.c['qos'])],
                subscriptions=[dict(endpoint_gid=self.endpoints['reader']['endpoint_gid'],qos=self.c['qos'])], **identity)
            self.emit('command_done', role=role, command_id=self.serial, **identity)
            matched[role], graphs[role] = match['raw_event_ref'], graph['raw_event_ref']
        return dict(matches=matched, graphs=graphs)

    def command_record(self, argv, stdout=''):
        return dict(argv=argv, returncode=0, stdout=stdout,
                    net_namespace=self.isolation['net_namespace'],
                    started_monotonic_ns=self.clock.now, ended_monotonic_ns=self.clock.now)

    def counter(self, packets):
        table = self.tables[-1]
        data = {'nftables': [dict(counter=dict(family='inet', table=table, name='discovery_drop',
                                               packets=packets, bytes=packets*8)),
            dict(rule=dict(family='inet', table=table, expr=[
                {'match': {'left': {'meta': {'key': 'iifname'}}, 'right': 'lo'}},
                {'match': {'left': {'payload': {'protocol': 'udp', 'field': 'dport'}},
                           'op': '==', 'right': {'set': sorted(self.ports)}}}, {'drop': None}]))]}
        return dict(table=table, counter_observed=True, packets=packets, bytes=packets*8, hit=packets>0,
            nft_json=data, command=self.command_record(['nft','-j','list','table','inet',table], json.dumps(data)))

    def start(self, *, match_receipt):
        self.installs.append(self.clock.now)
        self.tables.append('r2f_discovery_' + f'{len(self.installs):016x}')
        start = self.clock.now
        self.clock.advance(self.install_delay)
        self.begin = dict(scope='ros2_pair', ros2_execution=True, configured=True, applied=True,
            table=self.tables[-1], precondition=deepcopy(match_receipt), pair_bindings=deepcopy(self.bindings),
            drop_begin_monotonic_ns=start, drop_applied_monotonic_ns=self.clock.now,
            install_command=self.command_record(['nft','-f','-']), counter_snapshot=self.counter(0))
        return deepcopy(self.begin)

    def counters(self):
        return self.counter(0 if self.zero_window == len(self.installs)-1 else 1)

    def restore(self):
        self.restores.append(self.clock.now)
        self.end = dict(restored=True, table_absent=True, table=self.tables[-1],
            drop_end_monotonic_ns=self.clock.now, counter_snapshot=self.counters(),
            delete_command=self.command_record(['nft','delete','table','inet',self.tables[-1]]),
            verify_command=self.command_record(['nft','-j','list','ruleset'], '{"nftables": []}'),
            final_nft={'nftables': []})
        return deepcopy(self.end)

    def confirm_recovery(self, *, activity_receipt):
        return dict(recovery_verified=True, restore_end_monotonic_ns=self.end['drop_end_monotonic_ns'],
            activity_receipt=deepcopy(activity_receipt), pair_bindings=deepcopy(self.bindings))

    def activity(self, since, label, **kw):
        self.clock.advance(0.06)
        return self.emit('discovery_activity_observation', window=label, activity=dict(until_ns=self.clock.time_ns()),
            activity_observed=True, after_monotonic_ns=kw.get('after_monotonic_ns'),
            clock_guard_ns=ns.CLOCK_GUARD_NS if label.startswith('post_restore') else 0)

    def step(self, name, requires, action, **kw):
        self.emit('state_step_before', step_id=name, requires=requires)
        evidence = action()
        self.emit('state_step_after', step_id=name, requires=requires, satisfied=True, evidence=evidence,receipt_kind='observation')
        return evidence

    def run(self):
        with patch.object(ns, 'time', self.clock), patch.object(ns, '_RecoveryMatchTrajectory', FakeTrajectory):
            return ns._run_window_chain(self.c, True, dict(planned=True, applied=False), self.emit,
                self.step, self.activity, lambda *a, **k: True, self.snapshot_pair, self.clock.advance,
                self, self.pool, self.events, self.endpoints, self.matched, self.precondition,
                self.match_receipt, *self.collections)

    def finish(self, root):
        """Fake public delivery and raw file; no actual SUT execution claim."""
        c = self.c
        self.emit('delivery_contract', expected_sequences=list(range(c['pre_match_count'], c['pre_match_count']+c['post_match_count'])),
            sentinel_sequence=c['pre_match_count']+c['post_match_count']-1, phase='post_restore',
            qos=c['qos'], prejoin_in_delivery_contract=False)
        self.emit('window_open', window='post_restore_delivery')
        for seq in range(c['pre_match_count'], c['pre_match_count']+c['post_match_count']):
            self.clock.advance(0.001)
            self.emit('send', role='writer',pid=self.endpoints['writer']['pid'],rmw_identifier=c['rmw'],
                      phase='post_restore', send_sequence=seq, publish_returned=True,
                      writer_gid=self.endpoints['writer']['endpoint_gid'],
                      endpoint_generation=self.endpoints['writer']['endpoint_generation'])
            self.emit('receive', role='reader',pid=self.endpoints['reader']['pid'],rmw_identifier=c['rmw'],
                phase='post_restore', send_sequence=seq, payload_run_id=c['run_id'],
                reader_gid=self.endpoints['reader']['endpoint_gid'],
                endpoint_generation=self.endpoints['reader']['endpoint_generation'],
                publisher_gid=self.endpoints['writer']['endpoint_gid'])
        self.emit('window_close', window='post_restore_delivery')
        self.step('observe_final_graph', [], lambda: self.snapshot_pair(self.clock.now))
        ns._emit_f2_operation_receipts(c, self.events, self.emit)
        self.emit('discovery_cleanup_restore', receipt=dict(restored=True))
        self.emit('namespace_controller_exited', returncode=0)
        self.emit('runtime_end',real_execution=True,runtime_status='completed')
        path = Path(root)/'raw.jsonl'
        for e in self.events:
            e['event_index'] = self.events.index(e)
        path.write_text(''.join(json.dumps(e, sort_keys=True)+'\n' for e in self.events))
        sha = hashlib.sha256(path.read_bytes()).hexdigest()
        ops=[]
        for e in self.events:
            if e['event'] == 'f2_operation_receipt':
                refs=[next(v for v in self.events if v['raw_event_ref']==r) for r in e['receipt']['evidence_refs']]+[e]
                ops.append(dict(e['receipt'], raw_event_ref=e['raw_event_ref'], raw_refs=[dict(path=str(path),sha256=sha,event_index=v['event_index']) for v in refs]))
        return dict(raw_path=str(path), raw_sha256=sha, operation_receipts=ops,
                    real_execution=True, runtime_status='completed', events=self.events)


class F2Timing(unittest.TestCase):
    def test_real_chain_uses_anchor_not_now_and_interval_max_not_sum(self):
        for count in (1,2,3):
            with self.subTest(count=count):
                h=Harness(configuration(count, offsets=[0.4]*count, interval=0.3))
                anchor=h.precondition['monotonic_ns']
                h.clock.advance(0.15)
                h.run()
                timing=[e for e in h.events if e['event']=='discovery_window_timing']
                self.assertEqual(h.installs[0], anchor+400_000_000)
                for i,e in enumerate(timing):
                    self.assertEqual(e['status'],'observed')
                    self.assertEqual(e['actual_start_monotonic_ns'], e['earliest_due_monotonic_ns'])
                    if i:
                        self.assertEqual(e['earliest_due_monotonic_ns']-e['anchor_monotonic_ns'],400_000_000)
                self.assertEqual(len(h.installs),count)
                self.assertEqual(len(h.restores),count)
                self.assertEqual(h.events[-1]['event'],'discovery_window_ledger')

    def test_prior_restore_interval_can_dominate_offset(self):
        h=Harness(configuration(2, offsets=[0.1,0.05], interval=0.4));h.run()
        e=[e for e in h.events if e['event']=='discovery_window_timing'][1]
        self.assertEqual(e['earliest_due_monotonic_ns']-e['anchor_monotonic_ns'],400_000_000)

    def test_install_lateness_is_harness_miss_and_restores_before_abort(self):
        h=Harness(configuration(1));h.install_delay=0.6
        with self.assertRaisesRegex(RuntimeError,'harness_anchor_timing_miss'):h.run()
        self.assertEqual(len(h.restores),1)
        e=next(e for e in h.events if e['event']=='discovery_window_timing')
        self.assertEqual(e['status'],'incomplete');self.assertEqual(e['overshoot_ns'],600_000_000)
        self.assertFalse(any(e['event']=='f2_operation_receipt' for e in h.events))

    def test_missing_wrong_future_anchor_or_generation_never_installs(self):
        for kind in ('missing','future','role','generation','namespace','unmatched'):
            with self.subTest(kind=kind):
                h=Harness(configuration(1))
                if kind=='missing':h.precondition['raw_event_ref']='not-in-raw';h.precondition=deepcopy(h.precondition);h.events.pop()
                if kind=='future':h.precondition['monotonic_ns']+=1
                if kind=='role':h.precondition['role']='reader'
                if kind=='generation':next(e for e in h.events if e['event']=='command_done')['endpoint_generation']='old'
                if kind=='namespace':h.precondition['bindings']['isolation']['net_namespace']='wrong'
                if kind=='unmatched':next(e for e in h.events if e['event']=='match_snapshot')['matched_count']=0
                with self.assertRaises((RuntimeError,KeyError)):h.run()
                self.assertFalse(h.installs)

    def test_unrecovered_previous_window_never_allows_second_install(self):
        h=Harness(configuration(2))
        with patch.object(h,'confirm_recovery',return_value=dict(recovery_verified=False)):
            with self.assertRaises((RuntimeError,KeyError)):h.run()
        self.assertEqual(len(h.installs),1)

    def test_new_route_dispatches_without_running_namespace(self):
        import tempfile
        c=configuration(1)
        with tempfile.TemporaryDirectory() as root, patch.object(ns,'run_discovery_pair',return_value={'mock_route':True}) as routed:
            self.assertEqual(pair.run_pair(c,Path(root)),{'mock_route':True})
            self.assertEqual(routed.call_args.args[0]['target_id'],pair.F2_WINDOWS_TARGET)

    def test_only_new_route_exposes_explicit_f2_capability(self):
        capabilities = pair.exploratory_runtime_capabilities()
        route = capabilities[pair.F2_WINDOWS_TARGET]
        self.assertEqual(route['runtime_capability'], 'f2_windows_v1')
        self.assertTrue(route['implemented'])
        self.assertEqual(route['backend'], 'discovery_namespace')
        self.assertEqual(route['carrier'], 'wire_proxy')
        self.assertEqual(route['mutation_kind'], 'discovery_drop')
        self.assertTrue(route['multi_window'])
        self.assertEqual(route['max_runtime_windows'], 3)
        self.assertEqual(route['surfaces'], ['E02', 'E03'])
        self.assertFalse(route['execution_evidence'])
        for target, legacy in capabilities.items():
            if target != pair.F2_WINDOWS_TARGET:
                with self.subTest(target=target):
                    self.assertNotIn('runtime_capability', legacy)

    def test_capability_policy_and_plan_binding_are_mandatory(self):
        for field,value in [('runtime_capability','legacy'),('anchor_timing_policy','legacy'),
                            ('anchor_lateness_tolerance_sec',1.0),('mutation_plan_sha256','0'*64),
                            ('final_input',{}),('fuzz_surfaces',['E01'])]:
            c=configuration(1);c[field]=value
            with self.subTest(field=field),self.assertRaises(ValueError):pair.f2_windows_operations(c)

    def test_legacy_target_does_not_gain_new_timing_policy_or_operation_gate(self):
        c=pair.normalize_config(dict(target_id='discovery.interruption_recovery',carrier='wire_proxy',mutation_kind='discovery_drop'))
        old=ns.normalize_discovery_config(c)
        self.assertNotIn('anchor_timing_policy',old)
        self.assertEqual(pair.f2_windows_operations(old),[])
        self.assertFalse(old['window_chain_requested'])


class F2AffectedAnchors(unittest.TestCase):
    def test_previous_restore_class_binds_all_and_only_later_windows(self):
        for count in (2, 3):
            c=configuration(count,'E03',offsets=[0.0]+[0.4]*(count-1),anchor='previous_restore_confirmed')
            self.assertEqual(pair.f2_windows_operations(c)[0]['affected_window_indices'],list(range(1,count)))
            bindings=pair.exploratory_bindings(c)
            pair.require_binding_preserved(bindings,c)
            self.assertTrue(all(b['path'][1] in range(1,count) for b in bindings))
            h=Harness(c);h.run()
            timing=[e for e in h.events if e['event']=='discovery_window_timing']
            self.assertEqual(timing[0]['trigger_anchor'],'fresh_match')
            self.assertEqual(timing[0]['earliest_due_monotonic_ns'],h.precondition['monotonic_ns'])
            for e in timing[1:]:
                self.assertEqual(e['earliest_due_monotonic_ns']-e['anchor_monotonic_ns'],400_000_000)

    def test_previous_restore_requested_with_one_window_is_rejected(self):
        c=configuration(1,'E03')
        c['mutation_plan']['operations'][0]['parameters']['anchor']='previous_restore_confirmed'
        c['submitted_parameters']['environment_windows.anchor']='previous_restore_confirmed'
        c['mutation_plan_sha256']=canonical_hash(c['mutation_plan'])
        with self.assertRaisesRegex(ValueError,'anchor_or_offset_mismatch'):pair.f2_windows_operations(c)

    def test_runtime_never_discharges_pending_guard_from_plan_boolean(self):
        c=configuration(1,'E03')
        c['mutation_plan']['operations'][0]['parameters']['anchor_observed']=True
        c['mutation_plan_sha256']=canonical_hash(c['mutation_plan'])
        with self.assertRaisesRegex(ValueError,'runtime_guard_not_pending'):pair.f2_windows_operations(c)
