"""Offline bridge-intake qualification; no ROS is started by these tests."""
import copy
import hashlib
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
from test_runtime_instrumentation import PairFixture, RMW, FASTDDS_RMW
from r2f.campaign.observations import qualify_observation, observation_links, instrumentation_evidence
from r2f.core import compile_scenario
from r2f.core.replay import ReplayCapsule
from r2f.modules.s1.module import S1Module
from r2f.reports.candidate_report import candidate_ledger


def capsule_and_overrides(rmw=RMW):
    source = {'head':'offline', 'runtime_source_sha256':'b' * 64}
    submitted = {'payload':{'kind':'normal'}, 'qos':{'reliability':'reliable'},
                 'topic':'/r2f_probe', 'message_type':'std_msgs/msg/String'}
    scenario = compile_scenario(S1Module().targets['discovery.late_endpoint'], rmw,
                                'baseline', 0, seed=23)
    capsule = ReplayCapsule.from_scenario(scenario, source_manifest=source,
                                          final_input=submitted, origin_domain=174)
    overrides = {'effective_input_sha256':capsule.input['effective_input_sha256'],
                 'final_input':submitted, 'payload':submitted['payload'], 'qos':submitted['qos'],
                 'topic':submitted['topic'], 'message_type':submitted['message_type'],
                 'input_layer_receipts':capsule.input['effective_input']['input_layer_receipts']}
    return capsule, overrides


def row_for(pair, runtime):
    return {'cell_id':'cyclone.baseline.0','run_id':pair.c['run_id'],'target_id':pair.c['target_id'],
            'target_revision':0,'rmw':pair.c['rmw'],'arm':'baseline','repeat':0,
            'domain_id':pair.c['domain_id'],'seed':pair.c['seed'],
            'source_manifest':runtime['execution']['source'],'effective_input_sha256':pair.c['effective_input_sha256'],
            'actual_run':True,'execution_stage':'judged','oracle_verdict':'pass',
            'raw_path':runtime['raw_path'],'raw_sha256':runtime['raw_sha256']}


def rehash_ref(ref, value):
    path=Path(ref['raw_path'])
    path.write_text(json.dumps(value,sort_keys=True,separators=(',',':'))+'\n')
    ref['raw_sha256']=hashlib.sha256(path.read_bytes()).hexdigest()


def rehash_summary(runtime):
    observation=runtime['observation']
    rehash_ref(observation['artifact'],{k:v for k,v in observation.items() if k!='artifact'})


class ObservationIntake(unittest.TestCase):
    def setup_pair(self,tmp,mode='instrumentation',overrides=None,environment=None,**kw):
        capsule,inputs=capsule_and_overrides((overrides or {}).get('rmw',RMW))
        inputs.update(overrides or {})
        pair=PairFixture(tmp,mode=mode,overrides=inputs,environment=environment); self.addCleanup(pair.close)
        runtime=pair.ready(**kw);runtime['observation']=pair.ctx.collect(runtime)
        return pair,runtime,row_for(pair,runtime),capsule

    def test_fastdds_actual_default_blackbox_sidecar_qualifies_without_cyclone_profiles(self):
        with tempfile.TemporaryDirectory() as tmp:
            pair,runtime,row,capsule=self.setup_pair(tmp,mode='blackbox',overrides={'rmw':FASTDDS_RMW})
            q=qualify_observation(row,runtime,pair.c,capsule=capsule)
            self.assertEqual(q['qualification'],'blackbox_observed',q)
            self.assertEqual(q['profile_policy']['cyclonedds_profiles'],'not_applicable')
            self.assertEqual(q['profile_policy']['contract'],'fastdds.default_blackbox_absence/v1')
            self.assertEqual(q['non_tracing_profiles'],{})
            self.assertIsNone(runtime['observation']['blackbox_task'])
            self.assertIsNone(q['admissible_count'])
            row['instrumentation_observation']=q
            ledger=candidate_ledger([row])
            self.assertEqual(ledger['instrumentation_status'],'not_run')
            self.assertEqual(ledger['candidates'],[])
            self.assertEqual(ledger['failures'],[])
            self.assertEqual(ledger['instrumentation_admission_failures'],[])
            self.assertEqual(ledger['natural_candidate_count'],0)

    def test_fastdds_qualification_rejects_unknown_mode_rmw_and_forged_profile_applicability(self):
        for case in ('unknown_mode','unknown_rmw','instrumentation','policy','missing_policy',
                     'profile','trace_config','traces','projections','input','raw','endpoint_identity'):
            with self.subTest(case=case),tempfile.TemporaryDirectory() as tmp:
                pair,runtime,row,capsule=self.setup_pair(tmp,mode='blackbox',overrides={'rmw':FASTDDS_RMW})
                observation=runtime['observation']
                ref=observation['prepared'];prepared=json.loads(Path(ref['raw_path']).read_text())
                if case=='unknown_mode':pair.c['observation_mode']='unknown'
                if case=='unknown_rmw':pair.c['rmw']='unknown'
                if case=='instrumentation':pair.c['observation_mode']='instrumentation'
                if case=='policy':
                    observation['profile_policy']['cyclonedds_profiles']='required'
                    prepared['profile_policy']=copy.deepcopy(observation['profile_policy'])
                if case=='missing_policy':
                    observation.pop('profile_policy');prepared.pop('profile_policy')
                if case=='profile':prepared['profiles']={'writer':{'xml':'<CycloneDDS/>'}}
                if case=='trace_config':prepared['trace_configs']={'writer':{'output_file':'fake'}}
                if case=='traces':observation['traces']=[{'fake':True}]
                if case=='projections':observation['reconciliations']=[{'fake':True}]
                if case=='input':observation['execution_input']['rmw']=RMW
                if case=='raw':row['raw_sha256']='f'*64
                if case=='endpoint_identity':
                    ref_worker=observation['process_captures']['writer']
                    capture=json.loads(Path(ref_worker['raw_path']).read_text())
                    capture['endpoint_event']['rmw_identifier']=RMW
                    rehash_ref(ref_worker,capture)
                rehash_ref(ref,prepared);rehash_summary(runtime)
                q=qualify_observation(row,runtime,pair.c,capsule=capsule)
                self.assertEqual(q['qualification'],'not_evaluated',q)
                self.assertTrue(q['errors'])
                self.assertFalse(q['zero_candidate'])

    def test_fastdds_rehashed_process_audit_cannot_hide_environment_library_map_or_fd_mismatch(self):
        cases=('environment','trace_environment','loader','fd','library','maps','device','defaults','missing_defaults','identity')
        for case in cases:
            with self.subTest(case=case),tempfile.TemporaryDirectory() as tmp:
                pair,runtime,row,capsule=self.setup_pair(tmp,mode='blackbox',overrides={'rmw':FASTDDS_RMW})
                ref=runtime['observation']['process_captures']['writer']
                capture=json.loads(Path(ref['raw_path']).read_text())
                audit=capture['audit'];proof=capture['process']
                if case=='environment':audit['environment']['FASTDDS_UNKNOWN_PROFILE']=''
                if case=='trace_environment':audit['environment']['CYCLONEDDS_URI']='<CycloneDDS><Tracing/></CycloneDDS>'
                if case=='loader':audit['environment']['LD_PRELOAD']='trace.so'
                if case=='fd':audit['fd_targets'].append(str(pair.ctx.directory/'fake.trace.log'))
                if case=='library':proof['loaded_libraries']=[]
                if case=='maps':audit['maps']=''
                if case=='device':proof['loaded_libraries'][0]['mapped_device']='ff:ff'
                if case=='defaults':audit['default_profile_files']['DEFAULT_FASTRTPS_PROFILES.xml']['exists']=True
                if case=='missing_defaults':audit.pop('default_profile_files')
                if case=='identity':audit['environment']['RMW_IMPLEMENTATION']=RMW
                rehash_ref(ref,capture);rehash_summary(runtime)
                q=qualify_observation(row,runtime,pair.c,capsule=capsule)
                self.assertEqual(q['qualification'],'not_evaluated',q)
                self.assertTrue(q['errors'])
                self.assertIsNone(q['admissible_count'])

    def test_actual_multisegment_sidecars_qualify_for_both_rmws_and_modes(self):
        for rmw,mode in ((RMW,'instrumentation'),(RMW,'blackbox'),(FASTDDS_RMW,'blackbox')):
            with self.subTest(rmw=rmw,mode=mode),tempfile.TemporaryDirectory() as tmp:
                pair,runtime,row,capsule=self.setup_pair(tmp,mode=mode,overrides={'rmw':rmw})
                for ref in runtime['observation']['process_captures'].values():
                    capture=json.loads(Path(ref['raw_path']).read_text())
                    libraries=capture['process']['loaded_libraries']
                    self.assertEqual(len(capture['audit']['maps'].splitlines()),5*len(libraries))
                q=qualify_observation(row,runtime,pair.c,capsule=capsule)
                expected='blackbox_observed' if mode=='blackbox' else 'qualified'
                self.assertEqual(q['qualification'],expected,q)

    def test_rehashed_maps_reject_each_segment_conflict_and_exact_path_mismatch(self):
        for rmw,mode in ((RMW,'instrumentation'),(RMW,'blackbox'),(FASTDDS_RMW,'blackbox')):
            for case in ('inode','device','absent','substring','nonexistent','mapped_device','mapped_inode'):
                with self.subTest(rmw=rmw,mode=mode,case=case),tempfile.TemporaryDirectory() as tmp:
                    pair,runtime,row,capsule=self.setup_pair(tmp,mode=mode,overrides={'rmw':rmw})
                    ref=runtime['observation']['process_captures']['writer']
                    capture=json.loads(Path(ref['raw_path']).read_text())
                    library=capture['process']['loaded_libraries'][0];path=library['mapped_path']
                    fields=[line.split(maxsplit=5) for line in capture['audit']['maps'].splitlines()]
                    segments=[f for f in fields if f[5]==path]
                    if case=='inode':segments[-1][4]=str(int(segments[-1][4])+1)
                    if case=='device':segments[-1][3]='ff:ff'
                    if case=='absent':fields=[f for f in fields if f[5]!=path]
                    if case in ('substring','nonexistent'):
                        replacement=path+('.collision' if case=='substring' else '.missing')
                        for f in segments:f[5]=replacement
                        if case=='nonexistent':library['mapped_path']=replacement
                    if case=='mapped_device':library['mapped_device']='ff:ff'
                    if case=='mapped_inode':library['mapped_inode']+=1
                    capture['audit']['maps']=''.join(' '.join(f)+'\n' for f in fields)
                    rehash_ref(ref,capture);rehash_summary(runtime)
                    q=qualify_observation(row,runtime,pair.c,capsule=capsule)
                    self.assertEqual(q['qualification'],'not_evaluated',q)
                    self.assertIn('loaded binary differs from saved process maps',q['errors'])
                    self.assertIsNone(q['admissible_count'])
                    self.assertFalse(q['zero_candidate'])

    def test_actual_blackbox_sidecar_qualifies_without_a_planned_task(self):
        with tempfile.TemporaryDirectory() as tmp:
            pair,runtime,row,capsule=self.setup_pair(tmp,mode='blackbox')
            self.assertIsNone(runtime['observation']['blackbox_task'])
            self.assertEqual(runtime['observation']['blackbox_observer']['status'],'verified')
            q=qualify_observation(row,runtime,pair.c,capsule=capsule)
            self.assertEqual(q['qualification'],'blackbox_observed',q)
            self.assertEqual(q['input_binding']['capsule_validation'],'verified')
            self.assertEqual(q['input_binding']['sidecar_binding'],
                             'execution_input_plus_prepared_and_verified_raw_sha256')
            self.assertIsNone(q['admissible_count'])
            self.assertFalse(q['zero_candidate'])
            self.assertEqual(q['qualified_intake'],[])
            missing_capsule=qualify_observation(row,runtime,pair.c)
            self.assertIn('validated replay capsule required for observation qualification',missing_capsule['errors'])
            row['instrumentation_observation']=q
            ledger=candidate_ledger([row])
            self.assertEqual(ledger['candidates'],[])
            self.assertIsNone(ledger['instrumentation_qualified_intake_count'])
            self.assertEqual(ledger['instrumentation_admission_failures'],[])
            self.assertEqual(ledger['natural_candidate_count'],0)

    def test_execution_input_is_required_and_each_field_is_bound_in_both_modes(self):
        from r2f.campaign.observations import _OBSERVATION_INPUT
        for mode in ('instrumentation','blackbox'):
            with tempfile.TemporaryDirectory() as tmp:
                pair,runtime,row,capsule=self.setup_pair(tmp,mode=mode)
                for field in (None,*_OBSERVATION_INPUT):
                    for mutation in ('missing','tampered'):
                        with self.subTest(mode=mode,field=field,mutation=mutation):
                            changed=copy.deepcopy(runtime)
                            if field is None:
                                changed['observation'].pop('execution_input')
                            elif mutation=='missing':
                                changed['observation']['execution_input'].pop(field)
                            else:
                                changed['observation']['execution_input'][field]={'forged':True}
                            rehash_summary(changed)
                            q=qualify_observation(row,changed,pair.c,capsule=capsule)
                            self.assertEqual(q['qualification'],'not_evaluated')
                            self.assertIsNone(q['admissible_count'])
                            self.assertTrue(any('execution_input' in e for e in q['errors']),q)
                            self.assertFalse(q['zero_candidate'])

    def test_execution_input_rejects_json_type_substitution(self):
        for mode in ('instrumentation','blackbox'):
            with self.subTest(mode=mode), tempfile.TemporaryDirectory() as tmp:
                pair,runtime,row,capsule=self.setup_pair(tmp,mode=mode)
                runtime['observation']['execution_input']['repeat']=False
                rehash_summary(runtime)
                q=qualify_observation(row,runtime,pair.c,capsule=capsule)
                self.assertIn('observation execution_input differs:repeat',q['errors'])

    def test_raw_byte_change_is_not_hidden_by_matching_metadata(self):
        for mode in ('instrumentation','blackbox'):
            with self.subTest(mode=mode), tempfile.TemporaryDirectory() as tmp:
                pair,runtime,row,capsule=self.setup_pair(tmp,mode=mode)
                raw=Path(runtime['raw_path'])
                raw.write_bytes(raw.read_bytes()+b'\n')
                q=qualify_observation(row,runtime,pair.c,capsule=capsule)
                self.assertEqual(q['qualification'],'not_evaluated')
                self.assertTrue(q['errors'])
                self.assertFalse(q['zero_candidate'])

    def test_execution_input_is_bound_to_the_prepared_artifact(self):
        for mode in ('instrumentation','blackbox'):
            with self.subTest(mode=mode), tempfile.TemporaryDirectory() as tmp:
                pair,runtime,row,capsule=self.setup_pair(tmp,mode=mode)
                ref=runtime['observation']['prepared']
                prepared=json.loads(Path(ref['raw_path']).read_text())
                prepared['execution_input']['seed']+=1
                rehash_ref(ref,prepared)
                rehash_summary(runtime)
                q=qualify_observation(row,runtime,pair.c,capsule=capsule)
                self.assertIn('prepared execution_input differs from observation',q['errors'])

    def test_planned_task_gate_is_exclusive_to_instrumentation(self):
        for mode in ('instrumentation','blackbox'):
            with tempfile.TemporaryDirectory() as tmp:
                pair,runtime,row,capsule=self.setup_pair(tmp,mode=mode)
                for mutation in ('status','executed','origin','same_input','profiles','missing'):
                    with self.subTest(mode=mode,mutation=mutation):
                        changed=copy.deepcopy(runtime)
                        if mode=='blackbox':
                            changed['observation']['blackbox_task']={'status':'not_run','executed_by_bridge':False}
                        else:
                            task=changed['observation']['blackbox_task']
                            if mutation=='status':task['status']='executed'
                            if mutation=='executed':task['executed_by_bridge']=True
                            if mutation=='origin':task['origin_run_id']='another'
                            if mutation=='same_input':task['same_input']['seed']+=1
                            if mutation=='profiles':task['non_tracing_profiles']={}
                            if mutation=='missing':changed['observation']['blackbox_task']=None
                        rehash_summary(changed)
                        q=qualify_observation(row,changed,pair.c,capsule=capsule)
                        self.assertIsNone(q['admissible_count'])
                        self.assertTrue(q['errors'],q)
                        if mode=='blackbox':
                            self.assertIn('blackbox execution must not carry a planned task',q['errors'])

    def test_blackbox_and_instrumentation_bind_canonical_config_runtime_and_raw(self):
        for mode in ('instrumentation','blackbox'):
            with tempfile.TemporaryDirectory() as tmp:
                pair,runtime,row,capsule=self.setup_pair(tmp,mode=mode)
                for mutation in ('canonical_seed','missing_canonical_seed','config_seed','raw_hash','runtime_identity',
                                 'runtime_alias','unhashed_summary'):
                    with self.subTest(mode=mode,mutation=mutation):
                        changed,canonical,config=copy.deepcopy((runtime,row,pair.c))
                        if mutation=='canonical_seed':canonical['seed']+=1
                        if mutation=='missing_canonical_seed':canonical.pop('seed')
                        if mutation=='config_seed':config['seed']+=1
                        if mutation=='raw_hash':changed['raw_sha256']='f'*64
                        if mutation=='runtime_identity':changed['execution']['run_id']='other'
                        if mutation=='runtime_alias':
                            changed['execution_metadata']=dict(changed['execution'],domain_id=175)
                        if mutation=='unhashed_summary':changed['observation']['execution_input']['seed']+=1
                        q=qualify_observation(canonical,changed,config,capsule=capsule)
                        self.assertIsNone(q['admissible_count'])
                        self.assertEqual(q['qualification'],'not_evaluated')
                        self.assertTrue(q['errors'])

    def test_transport_profiles_cannot_be_rehashed_to_replace_process_transport(self):
        from r2f.runtime.instrumentation_bridge import _xml
        for mode in ('instrumentation','blackbox'):
            for mutation in ('transport','domain','hash','malformed','tracing'):
                with self.subTest(mode=mode,mutation=mutation), tempfile.TemporaryDirectory() as tmp:
                    pair,runtime,row,capsule=self.setup_pair(tmp,mode=mode)
                    ref=runtime['observation']['prepared']
                    prepared=json.loads(Path(ref['raw_path']).read_text())
                    profile=prepared['profiles']['writer']
                    if mutation=='transport':
                        profile['xml'],profile['non_tracing_sha256'],_= _xml(
                            profile['xml'].replace('<Transport>udp</Transport>','<Transport>tcp</Transport>'),174)
                    if mutation=='domain':profile['xml']=profile['xml'].replace('Id="any"','Id="42"')
                    if mutation=='hash':profile['non_tracing_sha256']='f'*64
                    if mutation=='malformed':profile['xml']='<CycloneDDS>'
                    if mutation=='tracing':profile['xml']=profile['xml'].replace('</Domain>','<Tracing/></Domain>')
                    rehash_ref(ref,prepared)
                    rehash_summary(runtime)
                    q=qualify_observation(row,runtime,pair.c,capsule=capsule)
                    self.assertEqual(q['qualification'],'not_evaluated')
                    self.assertTrue(q['errors'],q)
                    if mutation=='transport':
                        self.assertIn('prepared transport profiles differ from process environment',q['errors'])

    def test_completed_verified_zero_is_distinct_from_empty_intake(self):
        with tempfile.TemporaryDirectory() as tmp:
            pair,runtime,row,capsule=self.setup_pair(tmp)
            q=qualify_observation(row,runtime,pair.c,capsule=capsule)
            self.assertEqual(q['qualification'],'qualified',q)
            self.assertEqual(q['admissible_count'],0)
            self.assertEqual(q['input_binding']['capsule_validation'],'verified')
            self.assertEqual(q['input_binding']['runtime_binding'],
                             'execution_metadata_identity_plus_verified_raw_sha256')
            self.assertEqual(q['input_binding']['sidecar_binding'],
                             'execution_input_plus_prepared_and_verified_raw_sha256')
            self.assertNotIn('effective_input_sha256',runtime['execution'])
            row['instrumentation_observation']=q
            ledger=candidate_ledger([row])
            self.assertEqual(ledger['instrumentation_status'],'completed')
            self.assertEqual(ledger['instrumentation_candidates'],0)
            self.assertEqual(ledger['confirmed_candidates'],0)

    def test_not_run_and_unknown_count_remain_null(self):
        self.assertIsNone(qualify_observation({}, {}, {})['admissible_count'])
        with tempfile.TemporaryDirectory() as tmp:
            pair,runtime,row,capsule=self.setup_pair(tmp,missing_qos=True)
            q=qualify_observation(row,runtime,pair.c,capsule=capsule)
            self.assertEqual(q['intake_count'],0)
            self.assertIsNone(q['admissible_count'])
            self.assertFalse(q['zero_candidate'])
            row['instrumentation_observation']=q
            ledger=candidate_ledger([row])
            self.assertEqual(ledger['instrumentation_status'],'incomplete')
            self.assertIsNone(ledger['instrumentation_qualified_intake_count'])
            self.assertEqual(ledger['instrumentation_admitted_count'],0)
            self.assertEqual(ledger['instrumentation_lifecycle_status'],'incomplete')

    def test_rehashed_labels_do_not_replace_raw_provenance(self):
        with tempfile.TemporaryDirectory() as tmp:
            pair,runtime,row,capsule=self.setup_pair(tmp)
            for mode in ('summary','raw','source','canonical','binary'):
                item=copy.deepcopy(runtime); changed=copy.deepcopy(row)
                if mode=='summary': item['observation']['candidate_count']=7
                if mode=='raw': changed['raw_sha256']='0'*64
                if mode=='source':item['execution']['source_after']['head']='different'
                if mode=='canonical': changed['execution_stage']='planned'
                if mode=='binary':
                    lib=next(pair.libs.iterdir()); old=lib.read_bytes(); lib.write_bytes(b'changed')
                q=qualify_observation(changed,item,pair.c,capsule=capsule)
                self.assertIsNone(q['admissible_count'],mode)
                self.assertTrue(q['errors'],mode)
                if mode=='binary':lib.write_bytes(old)

    def test_same_input_planned_task_does_not_count_as_execution(self):
        with tempfile.TemporaryDirectory() as tmp:
            pair,runtime,row,capsule=self.setup_pair(Path(tmp)/'instrumentation')
            row['instrumentation_observation']=qualify_observation(row,runtime,pair.c,capsule=capsule)
            self.assertEqual(observation_links([row])[0]['status'],'not_run')
            black,actual,other,black_capsule=self.setup_pair(Path(tmp)/'blackbox',mode='blackbox',
                overrides={'run_id':'independent','domain_id':175})
            other['instrumentation_observation']=qualify_observation(other,actual,black.c,capsule=black_capsule)
            self.assertEqual(other['instrumentation_observation']['qualification'],'blackbox_observed',other)
            self.assertIsNone(actual['observation']['blackbox_task'])
            link=observation_links([row,other])[0]
            self.assertEqual(link['status'],'executed')
            self.assertEqual(link['candidate_confirmation'],'not_performed')
            self.assertEqual(runtime['observation']['blackbox_task']['status'],'not_run')
            ledger=candidate_ledger([row,other])
            self.assertEqual(ledger['instrumentation_qualified_intake_count'],0)
            self.assertEqual(ledger['instrumentation_admitted_count'],0)
            self.assertEqual(ledger['instrumentation_admission_failures'],[])
            self.assertEqual(ledger['natural_candidate_count'],0)
            self.assertEqual(ledger['confirmed_candidates'],0)

    def test_limited_fail_intake_is_preserved_when_core_canonical_admission_fails(self):
        with tempfile.TemporaryDirectory() as tmp:
            pair,runtime,row,capsule=self.setup_pair(tmp,durability='transient_local')
            q=qualify_observation(row,runtime,pair.c,capsule=capsule)
            self.assertEqual(q['qualification'],'qualified',q)
            self.assertEqual(q['admissible_count'],1)
            row['instrumentation_observation']=q
            row['typed_facts']={role:{'id':pair.endpoints[role]['endpoint_gid'],'generation':role+'-generation'} for role in ('writer','reader')}
            ledger=candidate_ledger([row])
            self.assertEqual(ledger['candidates'],[])
            self.assertEqual(ledger['instrumentation_intake_count'],1)
            self.assertEqual(ledger['instrumentation_qualified_intake_count'],1)
            self.assertEqual(ledger['instrumentation_admitted_count'],0)
            self.assertEqual(ledger['instrumentation_candidates'],0)
            self.assertEqual(ledger['instrumentation_lifecycle_status'],'admission_failed')
            lifecycle=ledger['instrumentation_observations'][0]['lifecycle_accounting']
            self.assertEqual(lifecycle['qualified_intake_count'],1)
            self.assertEqual(lifecycle['admitted_count'],0)
            self.assertEqual(lifecycle['status'],'admission_failed')
            row['instrumentation_observation']['qualified_intake'].append(
                copy.deepcopy(row['instrumentation_observation']['qualified_intake'][0]))
            row['instrumentation_observation']['admissible_count']=1
            inconsistent=candidate_ledger([row])
            self.assertEqual(inconsistent['instrumentation_status'],'incomplete')
            self.assertIsNone(inconsistent['instrumentation_qualified_intake_count'])
            self.assertEqual(inconsistent['instrumentation_observations'][0]['lifecycle_accounting']['status'],
                             'incomplete')
            self.assertEqual(inconsistent['candidates'],[])
            self.assertEqual(inconsistent['instrumentation_admitted_count'],0)
            generic=[f for f in inconsistent['failures']
                     if f['reason_code']=='instrumentation_qualified_intake_count_mismatch']
            dedicated=[f for f in inconsistent['instrumentation_admission_failures']
                       if f['reason_code']=='instrumentation_qualified_intake_count_mismatch']
            row_failures=inconsistent['instrumentation_observations'][0]['lifecycle_accounting']['admission_failures']
            self.assertEqual(len(generic),2)
            self.assertEqual([f['intake_index'] for f in generic],[0,1])
            self.assertTrue(all('intake' in f['evidence'] for f in generic))
            self.assertEqual(dedicated,generic)
            self.assertEqual(row_failures,generic)
            self.assertEqual(len(ledger['instrumentation_admission_failures']),1)
            failure=ledger['instrumentation_admission_failures'][0]
            self.assertEqual(failure['lifecycle']['candidate_status'],'observed')
            self.assertTrue(failure['detail'])
            self.assertEqual(ledger['confirmed_candidates'],0)

    def test_intake_identity_mismatch_stays_in_both_failure_ledgers(self):
        with tempfile.TemporaryDirectory() as tmp:
            pair,runtime,row,capsule=self.setup_pair(tmp,durability='transient_local')
            qualified=qualify_observation(row,runtime,pair.c,capsule=capsule)
            qualified['qualified_intake'][0]['run_id']='forged-run'
            row['instrumentation_observation']=qualified
            ledger=candidate_ledger([row])
            reason='instrumentation_intake_identity_mismatch'
            generic=[f for f in ledger['failures'] if f['reason_code']==reason]
            dedicated=[f for f in ledger['instrumentation_admission_failures'] if f['reason_code']==reason]
            row_failures=ledger['instrumentation_observations'][0]['lifecycle_accounting']['admission_failures']
            self.assertEqual(len(generic),1)
            self.assertEqual(generic[0]['intake_index'],0)
            self.assertEqual(dedicated,generic)
            self.assertEqual(row_failures,generic)
            self.assertEqual(ledger['candidates'],[])
            self.assertEqual(ledger['instrumentation_admitted_count'],0)
            self.assertEqual(ledger['instrumentation_lifecycle_status'],'admission_failed')

    def test_lifecycle_admission_failure_stays_in_both_failure_ledgers(self):
        with tempfile.TemporaryDirectory() as tmp:
            pair,runtime,row,capsule=self.setup_pair(tmp,durability='transient_local')
            row['instrumentation_observation']=qualify_observation(row,runtime,pair.c,capsule=capsule)
            row['typed_facts']={role:{'id':pair.endpoints[role]['endpoint_gid'],'generation':role+'-generation'}
                                for role in ('writer','reader')}
            reason='instrumentation_lifecycle_admission_failed'
            with patch('r2f.reports.candidate_report.CandidateLifecycle.admit',
                       side_effect=ValueError('controlled admission rejection')):
                ledger=candidate_ledger([row])
            generic=[f for f in ledger['failures'] if f['reason_code']==reason]
            dedicated=[f for f in ledger['instrumentation_admission_failures'] if f['reason_code']==reason]
            row_failures=ledger['instrumentation_observations'][0]['lifecycle_accounting']['admission_failures']
            self.assertEqual(len(generic),1)
            self.assertEqual(generic[0]['intake_index'],0)
            self.assertEqual(generic[0]['lifecycle']['candidate_status'],'observed')
            self.assertEqual(dedicated,generic)
            self.assertEqual(row_failures,generic)
            self.assertEqual(ledger['candidates'],[])
            self.assertEqual(ledger['instrumentation_admitted_count'],0)
            self.assertEqual(ledger['instrumentation_lifecycle_status'],'admission_failed')

    def test_lifecycle_observation_rejection_stays_in_failure_ledger(self):
        with tempfile.TemporaryDirectory() as tmp:
            pair,runtime,row,capsule=self.setup_pair(tmp,durability='transient_local')
            qualified=qualify_observation(row,runtime,pair.c,capsule=capsule)
            qualified['qualified_intake'][0]['timestamp']=''
            row['instrumentation_observation']=qualified
            row['typed_facts']={role:{'id':pair.endpoints[role]['endpoint_gid'],'generation':role+'-generation'}
                                for role in ('writer','reader')}
            ledger=candidate_ledger([row])
            self.assertEqual(ledger['candidates'],[])
            self.assertEqual(ledger['instrumentation_qualified_intake_count'],1)
            self.assertEqual(ledger['instrumentation_admitted_count'],0)
            self.assertEqual(ledger['instrumentation_lifecycle_status'],'admission_failed')
            self.assertEqual(ledger['instrumentation_admission_failures'][0]['reason_code'],
                             'instrumentation_lifecycle_observation_failed')
            self.assertTrue(ledger['failures'])

    def test_capsule_revalidation_rejects_matching_forged_row_and_config_hashes(self):
        with tempfile.TemporaryDirectory() as tmp:
            pair,runtime,row,capsule=self.setup_pair(tmp)
            forged='f'*64
            row['effective_input_sha256']=forged
            pair.c['effective_input_sha256']=forged
            with patch('r2f.campaign.observations.validate_replay_input', wraps=__import__(
                    'r2f.campaign.observations', fromlist=['validate_replay_input']).validate_replay_input) as validate:
                result=qualify_observation(row,runtime,pair.c,capsule=capsule)
            self.assertGreater(validate.call_count,0)
            self.assertIsNone(result['admissible_count'])
            self.assertIn('capsule effective input hash differs',result['errors'])

    def test_missing_canonical_domain_is_rejected_before_qualification(self):
        with tempfile.TemporaryDirectory() as tmp:
            pair,runtime,row,capsule=self.setup_pair(tmp)
            row.pop('domain_id')
            result=qualify_observation(row,runtime,pair.c,capsule=capsule)
            self.assertIsNone(result['admissible_count'])
            self.assertIn('canonical execution identity differs:domain_id',result['errors'])

    def test_missing_runtime_execution_identity_rejects_binding(self):
        with tempfile.TemporaryDirectory() as tmp:
            pair,runtime,row,capsule=self.setup_pair(tmp)
            runtime['execution'].pop('domain_id')
            result=qualify_observation(row,runtime,pair.c,capsule=capsule)
            self.assertIsNone(result['admissible_count'])
            self.assertIn('runtime execution identity differs:domain_id',result['errors'])

    def test_missing_sidecar_same_input_hash_rejects_even_with_rehashed_artifact(self):
        with tempfile.TemporaryDirectory() as tmp:
            pair,runtime,row,capsule=self.setup_pair(tmp)
            observation=runtime['observation']
            stored=json.loads(Path(observation['artifact']['raw_path']).read_text())
            def remove_same_input_hash(value):
                if isinstance(value,dict):
                    task=value.get('blackbox_task') or value.get('control_task')
                    if isinstance(task,dict) and isinstance(task.get('same_input'),dict):
                        task['same_input'].pop('effective_input_sha256',None)
                    for child in value.values():remove_same_input_hash(child)
                elif isinstance(value,list):
                    for child in value:remove_same_input_hash(child)
            remove_same_input_hash(stored)
            remove_same_input_hash(observation)
            artifact_path=Path(observation['artifact']['raw_path'])
            artifact_path.write_text(json.dumps(stored,sort_keys=True,separators=(',',':'))+'\n')
            observation['artifact']['raw_sha256']=hashlib.sha256(artifact_path.read_bytes()).hexdigest()
            result=qualify_observation(row,runtime,pair.c,capsule=capsule)
            self.assertIsNone(result['admissible_count'])
            self.assertIn('blackbox task effective input differs',result['errors'])

    def test_intake_hash_is_reasserted_before_lifecycle_admission(self):
        with tempfile.TemporaryDirectory() as tmp:
            pair,runtime,row,capsule=self.setup_pair(tmp,durability='transient_local')
            qualified=qualify_observation(row,runtime,pair.c,capsule=capsule)
            item=copy.deepcopy(qualified['qualified_intake'][0])
            item['effective_input_sha256']='f'*64
            with self.assertRaisesRegex(ValueError,'effective_input_sha256'):
                instrumentation_evidence(row,item)

    def test_blackbox_link_requires_equal_normalized_transport_profiles(self):
        from test_runtime_instrumentation import PROFILE
        with tempfile.TemporaryDirectory() as tmp:
            pair,runtime,row,capsule=self.setup_pair(Path(tmp)/'instrumentation')
            row['instrumentation_observation']=qualify_observation(row,runtime,pair.c,capsule=capsule)
            for mismatch in (False,True):
                env={'CYCLONEDDS_URI':PROFILE.replace('Id="any"','Id="175"')}
                if mismatch:
                    env['CYCLONEDDS_URI']=env['CYCLONEDDS_URI'].replace('<Transport>udp</Transport>',
                                                                             '<Transport>tcp</Transport>')
                black,actual,other,black_capsule=self.setup_pair(Path(tmp)/str(mismatch),mode='blackbox',
                    overrides={'run_id':'independent','domain_id':175},environment=env)
                other['instrumentation_observation']=qualify_observation(other,actual,black.c,capsule=black_capsule)
                self.assertEqual(other['instrumentation_observation']['qualification'],'blackbox_observed',other)
                link=observation_links([row,other])[0]
                self.assertEqual(link['status'],'profile_mismatch' if mismatch else 'executed')
                if mismatch:
                    self.assertEqual(link['blackbox_executions'],[])
                    self.assertEqual(link['rejected_blackbox_executions'][0]['reason'],
                                     'non_tracing_transport_profile_mismatch')

    def test_ingest_execution_forwards_capsule_to_observation_qualification(self):
        from r2f.campaign.runtime_adapter import ingest_execution
        from r2f.core.scenario import Scenario
        with tempfile.TemporaryDirectory() as tmp:
            capsule,overrides=capsule_and_overrides()
            pair=PairFixture(tmp,overrides=overrides); self.addCleanup(pair.close)
            runtime=pair.ready();runtime['observation']={}
            scenario=Scenario.from_dict(capsule.input['scenario'])
            captured={}
            def capture_row(canonical_row, observed_runtime, config, *, capsule=None):
                captured['row']=copy.deepcopy(canonical_row)
                captured['capsule']=capsule
                return {'qualification':'offline-spy'}
            with patch('r2f.campaign.observations.qualify_observation',side_effect=capture_row) as qualify:
                row=ingest_execution(scenario,pair.c,runtime,capsule)
            self.assertEqual(row['instrumentation_observation']['qualification'],'offline-spy')
            self.assertEqual(captured['row']['domain_id'],pair.c['domain_id'])
            self.assertEqual(row['domain_id'],pair.c['domain_id'])
            self.assertIs(captured['capsule'],capsule)
            self.assertIs(qualify.call_args.kwargs['capsule'],capsule)


if __name__=='__main__': unittest.main()
