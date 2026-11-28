"""Offline full fourth adapter contracts. These artifacts cannot seal a ROS run."""
from copy import deepcopy
import json
from pathlib import Path
import struct
import tempfile
import unittest
from test_instrumentation_sedp_projection import PREFIX, GUID, param, plist, packet, pcap, hashed, jsonraw
from r2f.core import compile_scenario, ReplayCapsule, canonical_hash
from r2f.campaign.identity import semantic_input, runtime_config
from r2f.campaign.runtime_adapter import ingest_execution
from r2f.campaign.triage import attribute
from r2f.modules.s1.module import S1Module
from r2f.core.oracle import validate_control
from r2f.runtime.projection_pair import controlled_graph, export_graph

TARGET='builtin.discovery_projection_reconciliation'
BASE=1700000000000000000


def projection_fixture(root,arm='baseline', *, graph_clocks=None):
    scenario=compile_scenario(S1Module().targets[TARGET],'rmw_cyclonedds_cpp',arm,0)
    final=semantic_input(arm,target_id=TARGET)
    capsule=ReplayCapsule.from_scenario(scenario,source_manifest={'head':'offline'},final_input=final)
    config=runtime_config(final,run_id='offline-projection',target_id=TARGET,target_revision=0,rmw=scenario.rmw,
                          arm=arm,repeat=0,domain_id=80,mutation_kind='endpoint_qos_variation',carrier='ros2_endpoint')
    events=[];pre=[];tick=1000
    def emit(event,role='controller',**fields):
        nonlocal tick
        tick+=1000
        e={'event':event,'event_index':len(events),'time_ns':BASE+tick,'monotonic_ns':tick,
           'run_id':config['run_id'],'domain_id':80,'role':role,'pid':100 if role=='controller' else 101 if role=='writer' else 102,
           'process_start_ticks':77,'endpoint_generation':role+'-generation',**deepcopy(fields)}
        e['raw_event_ref']=e['event_index'];events.append(e);return e
    emit('environment_preflight',ok=True);emit('isolation_locks_acquired',locks=['campaign','domain','state'])
    isolation={'isolated':True,'interfaces':['lo'],'net_namespace':'net:[2]','user_namespace':'user:[2]',
               'observed_net_namespace':'net:[2]','observed_user_namespace':'user:[2]',
               'host_net_namespace':'net:[1]','host_user_namespace':'user:[1]','host_uid':1000,
               'uid_map':[[0,1000,1]],'euid':0,'cap_eff':(1<<12)|(1<<13),'host_cap_eff':0}
    admission=emit('namespace_host_admission_verified',isolation=isolation)
    def step(name,deps,action):
        record={'step_id':name,'requires':deps,'before':{'monotonic_ns':tick,'satisfied':False},'satisfied':False}
        before=emit('state_step_before',**record)
        record['before']['raw_event_ref']=before['event_index']
        record['evidence']=action()
        record.update(satisfied=True,after={'monotonic_ns':tick,'satisfied':True})
        after=emit('state_step_after',**record)
        record['after']['raw_event_ref']=after['event_index'];pre.append(record)
    step('namespace_ready',['environment_ok','locks_held'],lambda:{'isolation':isolation,'rtps_probe':{'ok':True},'raw_event_ref':emit('namespace_ready',ready=True,drop_installed=False,isolation=isolation,namespace_rtps_probe={'ok':True})['event_index']})
    ready={}
    def observe():
        ready.update(status='ready',unix_ns=BASE+tick+1000,monotonic_ns=tick+1000)
        if graph_clocks is not None:
            ready['timestamp_clock']='kernel_SO_TIMESTAMPNS_realtime'
        return {'raw_event_ref':emit('discovery_observer_ready',ready=ready,receipt=ready)['event_index'],'ready':ready}
    step('observer_ready',['namespace_ready'],observe)
    mutation={k:arm=='mutated' for k in ('planned','submitted','carrier_admitted','configured','applied','hit','effect_observed')}
    begin=emit('mutation_begin' if arm=='mutated' else 'control_begin',preconditions_snapshot={'namespace_ready':True,'observer_ready':True,'endpoints_created':False},
               precondition_receipt_refs=[{'step_id':r['step_id'],'raw_before_index':r['before']['raw_event_ref'],'raw_after_index':r['after']['raw_event_ref']} for r in pre])
    emit('window_open',window='endpoint_creation',start_ns=begin['time_ns'])
    ids={'writer':GUID,'reader':'01100000000000000000000200001004'};endpoints={};calls={};records=[]
    for role in ('writer','reader'):
        calls[role]=emit('endpoint_create_begin',role,topic=config['topic'],qos=config['qos'],message_type=config['message_type'])
        wirelist=plist(extra=param(0x23,struct.pack('<iI',1,1<<30)) if arm=='mutated' else b'')
        wirelist=wirelist.replace(bytes.fromhex(GUID),bytes.fromhex(ids[role])).replace(b'rt/test',b'rt/test')
        # Wire topic length differs from runtime's default; keep an explicit test
        # topic in the submitted capsule rather than patching parser output.
        frame=packet(wirelist).replace(b'RTPS\x02\x01',b'RTPS\x02\x04')
        if role=='reader':frame=frame.replace(bytes.fromhex('000003c2'),bytes.fromhex('000004c2')).replace(bytes.fromhex(PREFIX),bytes.fromhex(ids[role][:24]))
        records.append((calls[role]['time_ns']+1,frame))
        endpoints[role]=emit('endpoint_created',role,topic=config['topic'],qos=config['qos'],message_type=config['message_type'],endpoint_gid=ids[role])
    # Fixture topic is /test in both independent sources and actual submitted API.
    config['topic']='/test';config['final_input']=dict(final,topic='/test')
    for e in events:
        if e['event'] in ('endpoint_create_begin','endpoint_created'):e['topic']='/test'
    capsule=ReplayCapsule.from_scenario(scenario,source_manifest={'head':'offline'},final_input=config['final_input'])
    graphs={};proofs=[]
    for i,role in enumerate(('writer','reader')):
        request=emit('graph_snapshot_requested',role,command_id=i+1,requested_monotonic_ns=tick+1000)
        match=emit('match_snapshot',role,command_id=i+1,endpoint_gid=ids[role],sampled_monotonic_ns=tick+1000,matched_count=0,count_api='get_subscription_count' if role=='writer' else 'get_publisher_count')
        graph=emit('graph_snapshot',role,command_id=i+1,
                   **({'timestamp_clock':graph_clocks[role]} if graph_clocks is not None and role in graph_clocks else {}),
                   publishers=[{'endpoint_gid':ids['writer'],'topic_type':config['message_type'],'qos':deepcopy(config['qos'])}],
                   subscriptions=[{'endpoint_gid':ids['reader'],'topic_type':config['message_type'],'qos':deepcopy(config['qos'])}])
        ack=emit('command_done',role,command_id=i+1)
        graphs[role]=graph
        proofs.append({'role':role,'command_id':i+1,'request_index':request['event_index'],'match_index':match['event_index'],'snapshot_index':graph['event_index'],'ack_index':ack['event_index']})
    graphval={'source':'ros_graph','captured_at_unix_ns':graphs['writer']['time_ns'],'endpoints':[
        {'endpoint_gid':ids[r],'role':r,'topic':'/test','topic_type':config['message_type'],'qos':deepcopy(config['qos'])} for r in ('writer','reader')]}
    if graph_clocks is None:
        graph={**deepcopy(graphval),'raw':jsonraw(root/'graph.json',graphval)}
    else:
        graph=export_graph({role:{'graph':value} for role,value in graphs.items()},
                           endpoints, config['topic'], root/'graph.json')
    export=emit('projection_graph_exported',graph_snapshot=graph,graph_receipts=proofs)
    window={'start_ns':begin['time_ns'],'end_ns':export['time_ns']}
    emit('window_close',window='endpoint_creation',**window)
    capture={'raw':hashed(root/'wire.pcap',pcap(records)), 'ready':ready,'unix_ns':BASE+tick+1000,
             'thread_stopped':True,'socket_closed':True,'health':{'status':'ok','kernel_drop_status':'known_at_stop'},
             'counters':{'kernel_drops':0},'limit_reason':None,'error':None}
    capture['receipt_raw']=jsonraw(root/'capture.json',deepcopy(capture))
    emit('discovery_observer_stopped',receipt=capture)
    observation={'capture_receipt':capture,'graph_snapshot':graph,'observation_window':window,'field_policy':{'protocol_defaults':True}}
    emit('projection_observation',**observation)
    if arm=='negative':emit('fault_control_receipt',**controlled_graph(graph,capture,root/'controlled.json'))
    emit('namespace_controller_exited',returncode=0)
    emit('runtime_end',mutation_receipt=mutation)
    raw=hashed(root/'raw.jsonl',b''.join(json.dumps(e).encode()+b'\n' for e in events))
    observation['endpoint_bindings']=[{'endpoint_guid':ids[r],'role':r,'pid':endpoints[r]['pid'],'process_start_ticks':77,'generation':r+'-generation',
        'begin_raw_ref':{**raw,'event_index':calls[r]['event_index']},'return_raw_ref':{**raw,'event_index':endpoints[r]['event_index']}} for r in ('writer','reader')]
    runtime={'runtime_status':'completed','real_execution':True,'evidence_origin':'fixture','environment':{'ok':True},
             'mutation_receipt':mutation,'precondition_receipts':pre,'events':events,'raw_path':raw['path'],'raw_sha256':raw['sha256'],
             'carrier_implementation':'isolated_netns_passive_sedp_public_qos','discovery_projection':observation,'execution_metadata':{'workers':[{'role':r,'pid':endpoints[r]['pid'],'returncode':0} for r in ('writer','reader')]}}
    return scenario,config,runtime,capsule


class ProjectionPipeline(unittest.TestCase):
    def test_plan_rejects_subset_excluding_requested_observation_cell(self):
        from r2f.campaign.runner import plan_campaign
        for mode, adopted in (('instrumentation', True), ('instrumentation', False),
                              ('blackbox', False)):
            with self.subTest(mode=mode, adopted=adopted), tempfile.TemporaryDirectory() as tmp:
                output = Path(tmp) / 'rejected'
                with self.assertRaisesRegex(ValueError,
                        'rmws selection excludes observation cell: rmw_cyclonedds_cpp.baseline.0'):
                    plan_campaign(output, rmws=['rmw_fastrtps_cpp'],
                        observation={'observation_mode': mode, 'observe_adopted_config': adopted})
                self.assertFalse((output / 'manifest.json').exists())
                self.assertFalse((output / 'cells').exists())
        with tempfile.TemporaryDirectory() as tmp:
            selection = {'observation_mode': 'instrumentation', 'observe_adopted_config': True}
            plan = plan_campaign(tmp, rmws=['rmw_cyclonedds_cpp'], observation=selection)
            row = plan['rows'][0]
            self.assertEqual(row['cell_id'], 'rmw_cyclonedds_cpp.baseline.0')
            config = json.loads(Path(row['runtime_config_path']).read_text())
            for key, value in selection.items():
                self.assertEqual(config[key], value)
            self.assertEqual(plan['observation'], dict(selection, cell_id=row['cell_id']))

    def test_execute_intake_rejects_excluded_observation_without_dispatch(self):
        from r2f.campaign.runner import run_campaign
        calls = []
        def forbidden_runtime(config, directory):
            calls.append(config['cell_id'])
            raise RuntimeError('offline sentinel: excluded observation must not dispatch')
        with tempfile.TemporaryDirectory() as tmp:
            output = Path(tmp) / 'rejected'
            with self.assertRaisesRegex(ValueError,
                    'rmws selection excludes observation cell: rmw_cyclonedds_cpp.baseline.0'):
                run_campaign(output, execute=True, max_executions=1, runtime=forbidden_runtime,
                    rmws=['rmw_fastrtps_cpp'], observation={
                        'observation_mode': 'instrumentation', 'observe_adopted_config': True})
            self.assertEqual(calls, [])
            self.assertFalse((output / 'manifest.json').exists())
            self.assertFalse((output / 'cells').exists())

    def test_canonical_cyclone_normal_control_preserves_discovery_layers(self):
        from r2f.campaign.runner import run_campaign
        cyclone = 'rmw_cyclonedds_cpp'
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            enabled = run_campaign(root / 'enabled', target_id=TARGET, rmws=[cyclone],
                parameters={'projection_discovery_layers': 'participant_topic_v1'},
                run_id='offline-layers-control', max_executions=1)
            manifest = json.loads(Path(enabled['manifest_path']).read_text())
            selection = manifest['rmw_selection']
            self.assertEqual(selection['plan_kind'], 'partial_rmw_plan')
            self.assertEqual(selection['selected_rmws'], [cyclone])
            self.assertTrue(selection['partial'])
            self.assertEqual(len(selection['uncovered_cell_ids']), 12)
            self.assertTrue(all(cell.startswith('rmw_fastrtps_cpp.')
                                for cell in selection['uncovered_cell_ids']))
            self.assertEqual(manifest['applicable_rmws_requested'],
                             list(S1Module().targets[TARGET].rmw_applicability))
            self.assertEqual(len(enabled['rows']), 12)
            self.assertEqual(enabled['required_cells'], 18)
            self.assertEqual(enabled['independent_fault_controls'], 6)
            self.assertEqual(enabled['full_scope_required_cells'], 72)
            self.assertFalse(enabled['maturity_denominator_changed'])
            self.assertFalse(enabled['seal']['sealed'])
            from r2f.reports.run_report import audit_report
            audit = audit_report(root / 'enabled' / 'report.json')
            self.assertFalse(audit['sealed'])
            self.assertIn('matrix_missing_duplicate_or_extra_cell', audit['errors'])
            self.assertIn('required_matrix_not_18', audit['errors'])
            self.assertEqual(audit['required_expected'], 18)
            self.assertEqual(audit['fault_expected'], 6)
            self.assertEqual(enabled['rmw_selection'], selection)
            row = enabled['rows'][0]
            self.assertEqual(row['cell_id'], cyclone + '.baseline.0')
            self.assertEqual(row['arm'], 'baseline')
            self.assertFalse(row['actual_run'])
            self.assertEqual(row['execution_stage'], 'planned')
            self.assertFalse(any(row['mutation_receipt'].values()))
            config = json.loads(Path(row['runtime_config_path']).read_text())
            capsule = ReplayCapsule.from_dict(json.loads(Path(row['replay_path']).read_text()))
            final = capsule.input['effective_input']['final_input']
            self.assertEqual(config['projection_discovery_layers'], 'participant_topic_v1')
            self.assertEqual(final['projection_discovery_layers'], 'participant_topic_v1')
            self.assertEqual(config['final_input'], final)
            self.assertEqual(config['qos'], config['baseline_qos'])
            self.assertIsNone(final['fault_id'])
            self.assertIsNone(capsule.execution)

    def test_discovery_layers_are_only_deterministic_observation_input(self):
        from r2f.campaign.runner import plan_campaign
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            plans = [plan_campaign(root / name, target_id=TARGET,
                         rmws=['rmw_cyclonedds_cpp'], run_id=name, domain_id=domain,
                         parameters=parameters)
                     for name, domain, parameters in (
                         ('plain', 19, {}),
                         ('enabled', 20, {'projection_discovery_layers': 'participant_topic_v1'}),
                         ('different-execution', 21, {'projection_discovery_layers': 'participant_topic_v1'}))]
            inputs = []
            for plan in plans:
                configs = {row['arm']: json.loads(Path(row['runtime_config_path']).read_text())
                           for row in plan['rows']}
                self.assertEqual(configs['baseline']['final_input'], configs['transparent']['final_input'])
                for arm in ('baseline', 'transparent', 'negative'):
                    self.assertEqual(configs[arm]['qos'], configs[arm]['baseline_qos'])
                self.assertIsNone(configs['baseline']['fault_id'])
                self.assertEqual(configs['negative']['fault_id'], 'controlled_projection_field_mismatch')
                capsule = json.loads(Path(plan['rows'][0]['replay_path']).read_text())
                inputs.append(capsule['input'])
            plain, enabled, other = inputs
            self.assertNotIn('projection_discovery_layers', plain['effective_input']['final_input'])
            expected = dict(plain['effective_input']['final_input'],
                            projection_discovery_layers='participant_topic_v1')
            self.assertEqual(enabled['effective_input']['final_input'], expected)
            self.assertNotEqual(plain['effective_input_sha256'], enabled['effective_input_sha256'])
            self.assertEqual(enabled['effective_input'], other['effective_input'])
            self.assertEqual(enabled['effective_input_sha256'], other['effective_input_sha256'])

    def test_rmw_selection_fails_closed_and_keeps_canonical_order(self):
        from r2f.campaign.runner import plan_campaign
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            invalid = ([], (), 'rmw_cyclonedds_cpp', {'rmw_cyclonedds_cpp'},
                       ['rmw_cyclonedds_cpp', 'rmw_cyclonedds_cpp'],
                       [None], [[]], ['rmw_unknown_cpp'], ['rmw_fastrtps_cpp', ''])
            for index, rmws in enumerate(invalid):
                with self.subTest(rmws=rmws), self.assertRaisesRegex(ValueError, 'rmws'):
                    plan_campaign(root / str(index), target_id=TARGET, rmws=rmws)
                self.assertFalse((root / str(index) / 'manifest.json').exists())
            applicable = list(S1Module().targets[TARGET].rmw_applicability)
            full = plan_campaign(root / 'all', target_id=TARGET, rmws=applicable[::-1])
            self.assertEqual(len(full['rows']), 24)
            self.assertEqual(full['rows'][0]['cell_id'], 'rmw_cyclonedds_cpp.baseline.0')
            self.assertEqual(full['rmw_selection']['selected_rmws'], applicable)
            self.assertFalse(full['rmw_selection']['partial'])
            self.assertEqual(full['rmw_selection']['uncovered_cell_ids'], [])

    def test_discovery_layer_tokens_and_fastdds_still_fail_closed(self):
        from r2f.campaign.runner import plan_campaign
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            for index, value in enumerate((None, '', 'other', False, 1, [], {})):
                with self.subTest(value=value), self.assertRaisesRegex(
                        ValueError, 'unsupported_projection_discovery_layers'):
                    plan_campaign(root / str(index), target_id=TARGET,
                        rmws=['rmw_cyclonedds_cpp'], parameters={'projection_discovery_layers': value})
            with self.assertRaisesRegex(ValueError, 'unsupported_projection_discovery_layers_rmw'):
                plan_campaign(root / 'fastdds', target_id=TARGET, rmws=['rmw_fastrtps_cpp'],
                    parameters={'projection_discovery_layers': 'participant_topic_v1'})

    def test_default_rmw_selection_keeps_legacy_plan_shape_and_inputs(self):
        from r2f.campaign.runner import plan_campaign
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            default = plan_campaign(root / 'default', target_id=TARGET, run_id='same-default-input')
            explicit_none = plan_campaign(root / 'none', target_id=TARGET, rmws=None,
                                          run_id='same-default-input')
            for plan in (default, explicit_none):
                self.assertEqual(len(plan['rows']), 24)
                self.assertEqual(plan['required_cells'], 18)
                self.assertEqual(plan['independent_fault_controls'], 6)
                self.assertEqual(plan['full_scope_required_cells'], 72)
                self.assertNotIn('rmw_selection', plan)
                manifest = json.loads(Path(plan['manifest_path']).read_text())
                self.assertNotIn('rmw_selection', manifest)
                for row in plan['rows']:
                    config = json.loads(Path(row['runtime_config_path']).read_text())
                    self.assertNotIn('projection_discovery_layers', config)
                    self.assertNotIn('projection_discovery_layers', config['final_input'])
            self.assertEqual(default['identity'], explicit_none['identity'])
            self.assertEqual([row['effective_input_sha256'] for row in default['rows']],
                             [row['effective_input_sha256'] for row in explicit_none['rows']])

    def test_plan_exact_config_neutral_identity_and_unchanged_first_two(self):
        from r2f.campaign.runner import plan_campaign
        with tempfile.TemporaryDirectory() as tmp:
            plan=plan_campaign(tmp,target_id=TARGET,parameters={'variant_qos':{'reliability':'reliable','durability':'transient_local','depth':64,'deadline_ns':9223372036854775807,'liveliness':'automatic','liveliness_lease_duration_ns':9223372036854775807}})
            self.assertEqual(len(plan['rows']),24);self.assertEqual(plan['full_scope_required_cells'],72)
            submitted={}
            for row in plan['rows']:
                c=json.loads(Path(row['runtime_config_path']).read_text());cap=ReplayCapsule.from_dict(json.loads(Path(row['replay_path']).read_text()))
                self.assertIsNone(cap.execution)
                self.assertEqual(c['final_input'],cap.input['effective_input']['final_input'])
                self.assertEqual(c['mutation_parameters']['submitted_qos'],c['qos'])
                submitted[row['arm']]=canonical_hash(c['final_input'])
            self.assertEqual(submitted['baseline'],submitted['transparent'])
        expected=['c67eb5837a34bb6970d4d59bf2cb339d1b6a07243af9d0ee949dbdc789533940','1f65b4dbfc7bc5c9be454251fa536705597c75a36981f4e23ba9e6e60076002d']
        for target,want in zip(list(S1Module().targets.values())[:2],expected):self.assertEqual(canonical_hash(target.to_dict()),want)

    def test_neutral_mutated_and_projection_fault_use_core_and_write_capsule(self):
        for arm in ('baseline','transparent','mutated','negative'):
            with self.subTest(arm=arm),tempfile.TemporaryDirectory() as tmp:
                s,c,r,cap=projection_fixture(Path(tmp),arm)
                row=ingest_execution(s,c,r,cap)
                self.assertEqual(row['execution_stage'],'judged',row.get('abort_reason'))
                self.assertEqual(row['oracle_verdict'],'fail' if arm=='negative' else 'pass',row['oracle'])
                cap.record_execution(row);self.assertEqual(cap.execution['raw_sha256'],r['raw_sha256'])
                if arm=='negative':
                    self.assertEqual(attribute(row,row['typed_facts'],row['oracle'])['responsibility_layer'],'harness')
                    self.assertTrue(validate_control(dict(row,control_kind='fault')))
                    self.assertNotIn('mutation_applied',[h['execution_stage'] for h in row['stage_history']])

    def test_hash_rebinding_does_not_admit_bad_chronology_or_unbound_graph(self):
        for mode in ('create_before_begin','graph_ack','snapshot','wire','binding'):
            with self.subTest(mode=mode),tempfile.TemporaryDirectory() as tmp:
                s,c,r,cap=projection_fixture(Path(tmp))
                if mode=='create_before_begin':next(e for e in r['events'] if e['event']=='endpoint_create_begin')['monotonic_ns']=0
                if mode=='graph_ack':next(e for e in r['events'] if e['event']=='command_done')['command_id']=999
                if mode=='snapshot':next(e for e in r['events'] if e['event']=='match_snapshot')['sampled_monotonic_ns']=0
                if mode=='wire':Path(r['discovery_projection']['capture_receipt']['raw']['path']).write_bytes(b'changed')
                if mode=='binding':r['discovery_projection']['endpoint_bindings'][0]['generation']='forged'
                raw=hashed(Path(r['raw_path']),b''.join(json.dumps(e).encode()+b'\n' for e in r['events']))
                r['raw_sha256']=raw['sha256']
                for b in r['discovery_projection']['endpoint_bindings']:
                    for key in ('begin_raw_ref','return_raw_ref'):b[key]['sha256']=raw['sha256']
                row=ingest_execution(s,c,r,cap)
                self.assertEqual(row['execution_stage'],'aborted',mode)
                self.assertEqual(row['oracle_verdict'],'unknown',mode)

    def test_natural_graph_disagreement_is_primary_fail_not_input_abort(self):
        with tempfile.TemporaryDirectory() as tmp:
            s,c,r,cap=projection_fixture(Path(tmp),'mutated')
            # Only the observed graph disagrees; submitted create arguments and
            # independently captured wire stay unchanged.
            for e in r['events']:
                if e['event']=='graph_snapshot':e['publishers'][0]['qos']['reliability']='best_effort'
            graph=r['discovery_projection']['graph_snapshot']
            value=json.loads(Path(graph['raw']['path']).read_text())
            value['endpoints'][0]['qos']['reliability']='best_effort'
            graph.update(value,raw=jsonraw(Path(graph['raw']['path']),value))
            for e in r['events']:
                if e['event'] in ('projection_observation','projection_graph_exported'):e['graph_snapshot']=deepcopy(graph)
            raw=hashed(Path(r['raw_path']),b''.join(json.dumps(e).encode()+b'\n' for e in r['events']))
            r['raw_sha256']=raw['sha256']
            for binding in r['discovery_projection']['endpoint_bindings']:
                for key in ('begin_raw_ref','return_raw_ref'):binding[key]['sha256']=raw['sha256']
            row=ingest_execution(s,c,r,cap)
            self.assertEqual(row['execution_stage'],'judged',row.get('abort_reason'))
            self.assertEqual(row['oracle_verdict'],'fail',row['oracle'])
            self.assertIn('builtin_graph_field_mismatch',row['oracle']['forbidden_residuals'])

    def test_fault_cannot_modify_second_field_or_become_mutation(self):
        with tempfile.TemporaryDirectory() as tmp:
            s,c,r,cap=projection_fixture(Path(tmp),'negative')
            fault=next(e for e in r['events'] if e['event']=='fault_control_receipt')
            path=Path(fault['controlled_projection_ref']);data=json.loads(path.read_text());data['endpoints'][0]['qos']['durability']='transient_local'
            ref=jsonraw(path,data);fault['controlled_projection_sha256']=ref['sha256']
            raw=hashed(Path(r['raw_path']),b''.join(json.dumps(e).encode()+b'\n' for e in r['events']));r['raw_sha256']=raw['sha256']
            for b in r['discovery_projection']['endpoint_bindings']:
                for key in ('begin_raw_ref','return_raw_ref'):b[key]['sha256']=raw['sha256']
            row=ingest_execution(s,c,r,cap)
            self.assertEqual(row['execution_stage'],'aborted');self.assertEqual(row['oracle_verdict'],'unknown')


if __name__=='__main__':unittest.main()


class GraphClockIngestionContract(unittest.TestCase):
    """Hash-bound offline fixtures exercise the public adapter, never a live run."""

    def test_raw_backed_clock_chain_qualifies_through_ingest_execution(self):
        for arm in ('baseline', 'transparent', 'mutated', 'negative'):
            with self.subTest(arm=arm), tempfile.TemporaryDirectory() as temp:
                s, c, r, cap = projection_fixture(Path(temp), arm, graph_clocks={
                    'writer': 'unix_ns_realtime', 'reader': 'unix_ns_realtime'})
                row = ingest_execution(s, c, r, cap)
                self.assertEqual(row['execution_stage'], 'judged', row.get('abort_reason'))
                self.assertEqual(row['oracle_verdict'], 'fail' if arm == 'negative' else 'pass')
                schema = row['typed_facts']['original_projection']['schema_check']
                self.assertEqual(schema['source_qualification']['timing']['status'], 'qualified')
                self.assertFalse(schema['all_layer_obligations_closed'])
                self.assertTrue(schema['endpoint_observations'])
                self.assertTrue(all(type(e['graph_minus_wire_ns']) is int
                                    for e in schema['endpoint_observations']))
                self.assertEqual(cap.input['source_manifest'], {'head': 'offline'})
                if arm == 'negative':
                    self.assertTrue(validate_control(dict(row, control_kind='fault')))
                    self.assertEqual(attribute(row, row['typed_facts'], row['oracle'])['responsibility_layer'], 'harness')
                    self.assertNotIn('mutation_applied', [h['execution_stage'] for h in row['stage_history']])
                    from r2f.core.candidate import CandidateLifecycle
                    evidence = dict(row, timestamp='2026-11-23T00:00:00+08:00',
                                    endpoint_identity='/'.join(row['typed_facts'][role]['id']
                                                               for role in ('writer', 'reader')),
                                    candidate_source='blackbox', evidence_tier='blackbox',
                                    provenance={'offline_fixture_only': True})
                    candidate = CandidateLifecycle('offline-clock-negative', evidence)
                    with self.assertRaisesRegex(ValueError, 'control rows cannot enter SUT candidates'):
                        candidate.admit()

    def test_legacy_missing_malformed_and_conflicting_clocks_remain_unknown(self):
        clocks = [None, {}, {'writer': 'unix_ns_realtime'}]
        clocks += [{'writer': 'unix_ns_realtime', 'reader': bad}
                   for bad in (None, True, 1, [], {}, 'monotonic_ns', 'unix_ns_realtime ')]
        for declared in clocks:
            with self.subTest(clocks=declared), tempfile.TemporaryDirectory() as temp:
                s, c, r, cap = projection_fixture(Path(temp), graph_clocks=declared)
                row = ingest_execution(s, c, r, cap)
                self.assertEqual(row['execution_stage'], 'judged', row.get('abort_reason'))
                self.assertEqual(row['oracle_verdict'], 'pass')
                self.assertNotIn('timestamp_clock', r['discovery_projection']['graph_snapshot'])
                schema = row['typed_facts']['original_projection']['schema_check']
                self.assertEqual(schema['source_qualification']['timing']['status'], 'unknown')
                self.assertTrue(all(e['graph_minus_wire_ns'] is None
                                    for e in schema['endpoint_observations']))

    def test_export_only_clock_raw_tamper_and_extra_fields_are_rejected(self):
        for mode in ('export_only', 'raw_tamper', 'rehashed_conflict', 'extra_field'):
            with self.subTest(mode=mode), tempfile.TemporaryDirectory() as temp:
                clocks = None if mode == 'export_only' else {
                    'writer': 'unix_ns_realtime', 'reader': 'unix_ns_realtime'}
                s, c, r, cap = projection_fixture(Path(temp), graph_clocks=clocks)
                graph = r['discovery_projection']['graph_snapshot']
                if mode in ('export_only', 'extra_field'):
                    graph['timestamp_clock' if mode == 'export_only' else 'unbound_clock_note'] = 'unix_ns_realtime'
                    value = {k: v for k, v in graph.items() if k != 'raw'}
                    graph['raw'] = jsonraw(Path(graph['raw']['path']), value)
                    for e in r['events']:
                        if e['event'] in ('projection_observation', 'projection_graph_exported'):
                            e['graph_snapshot'] = deepcopy(graph)
                else:
                    snapshots = [e for e in r['events'] if e['event'] == 'graph_snapshot']
                    snapshots[-1]['timestamp_clock'] = 'monotonic_ns'
                raw = hashed(Path(r['raw_path']), b''.join(
                    json.dumps(e).encode()+b'\n' for e in r['events']))
                if mode != 'raw_tamper':
                    r['raw_sha256'] = raw['sha256']
                    for binding in r['discovery_projection']['endpoint_bindings']:
                        for key in ('begin_raw_ref', 'return_raw_ref'):
                            binding[key]['sha256'] = raw['sha256']
                row = ingest_execution(s, c, r, cap)
                self.assertEqual(row['execution_stage'], 'aborted', mode)
                self.assertEqual(row['oracle_verdict'], 'unknown', mode)
                if mode != 'raw_tamper':
                    self.assertIn('independent graph JSON not reproduced', row['abort_reason'])
