"""Offline bridge-intake qualification; no ROS is started by these tests."""
import copy
import hashlib
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
from test_runtime_instrumentation import PairFixture, RMW
from r2f.campaign.observations import qualify_observation, observation_links, instrumentation_evidence
from r2f.core import compile_scenario
from r2f.core.replay import ReplayCapsule
from r2f.modules.s1.module import S1Module
from r2f.reports.candidate_report import candidate_ledger


def capsule_and_overrides():
    source = {'head':'offline', 'runtime_source_sha256':'b' * 64}
    submitted = {'payload':{'kind':'normal'}, 'qos':{'reliability':'reliable'},
                 'topic':'/r2f_probe', 'message_type':'std_msgs/msg/String'}
    scenario = compile_scenario(S1Module().targets['discovery.late_endpoint'], RMW,
                                'baseline', 0, seed=23)
    capsule = ReplayCapsule.from_scenario(scenario, source_manifest=source,
                                          final_input=submitted, origin_domain=174)
    overrides = {'effective_input_sha256':capsule.input['effective_input_sha256'],
                 'final_input':submitted, 'payload':submitted['payload'], 'qos':submitted['qos'],
                 'topic':submitted['topic'], 'message_type':submitted['message_type']}
    return capsule, overrides


def row_for(pair, runtime):
    return {'cell_id':'cyclone.baseline.0','run_id':pair.c['run_id'],'target_id':pair.c['target_id'],
            'target_revision':0,'rmw':pair.c['rmw'],'arm':'baseline','repeat':0,'domain_id':174,
            'source_manifest':runtime['execution']['source'],'effective_input_sha256':pair.c['effective_input_sha256'],
            'actual_run':True,'execution_stage':'judged','oracle_verdict':'pass',
            'raw_path':runtime['raw_path'],'raw_sha256':runtime['raw_sha256']}


class ObservationIntake(unittest.TestCase):
    def setup_pair(self,tmp,**kw):
        capsule,overrides=capsule_and_overrides()
        pair=PairFixture(tmp,overrides=overrides); self.addCleanup(pair.close)
        runtime=pair.ready(**kw);runtime['observation']=pair.ctx.collect(runtime)
        return pair,runtime,row_for(pair,runtime),capsule

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
                             'blackbox_task_same_input_plus_prepared_domain_and_verified_raw_sha256')
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
            pair,runtime,row,capsule=self.setup_pair(tmp)
            row['instrumentation_observation']=qualify_observation(row,runtime,pair.c,capsule=capsule)
            self.assertEqual(observation_links([row])[0]['status'],'not_run')
            other=copy.deepcopy(row);other.update(run_id='independent',domain_id=175,raw_path='different')
            other['instrumentation_observation']={'qualification':'blackbox_observed','non_tracing_profiles':copy.deepcopy(row['instrumentation_observation']['non_tracing_profiles'])}
            link=observation_links([row,other])[0]
            self.assertEqual(link['status'],'executed')
            self.assertEqual(link['candidate_confirmation'],'not_performed')
            self.assertEqual(runtime['observation']['blackbox_task']['status'],'not_run')

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
        with tempfile.TemporaryDirectory() as tmp:
            pair,runtime,row,capsule=self.setup_pair(tmp)
            row['instrumentation_observation']=qualify_observation(row,runtime,pair.c,capsule=capsule)
            other=copy.deepcopy(row)
            other.update(run_id='independent',domain_id=175,raw_path='different')
            profile=copy.deepcopy(row['instrumentation_observation']['non_tracing_profiles'])
            for role in profile:
                profile[role]['xml']=profile[role]['xml'].replace('Id="any"','Id="175"')
            other['instrumentation_observation']={'qualification':'blackbox_observed','non_tracing_profiles':profile}
            self.assertEqual(observation_links([row,other])[0]['status'],'executed')
            for role in profile:
                profile[role]['xml']=profile[role]['xml'].replace('<Transport>udp</Transport>',
                                                                    '<Transport>tcp</Transport>')
            mismatch=observation_links([row,other])[0]
            self.assertEqual(mismatch['status'],'profile_mismatch')
            self.assertEqual(mismatch['blackbox_executions'],[])
            self.assertEqual(mismatch['rejected_blackbox_executions'][0]['reason'],
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
