"""Offline bridge-intake qualification; no ROS is started by these tests."""
import copy
import json
from pathlib import Path
import tempfile
import unittest
from test_runtime_instrumentation import PairFixture
from r2f.campaign.observations import qualify_observation, observation_links
from r2f.reports.candidate_report import candidate_ledger


def row_for(pair, runtime):
    return {'cell_id':'cyclone.baseline.0','run_id':pair.c['run_id'],'target_id':pair.c['target_id'],
            'target_revision':0,'rmw':pair.c['rmw'],'arm':'baseline','repeat':0,'domain_id':174,
            'source_manifest':runtime['execution']['source'],'effective_input_sha256':pair.c['effective_input_sha256'],
            'actual_run':True,'execution_stage':'judged','oracle_verdict':'pass',
            'raw_path':runtime['raw_path'],'raw_sha256':runtime['raw_sha256']}


class ObservationIntake(unittest.TestCase):
    def setup_pair(self,tmp,**kw):
        pair=PairFixture(tmp); self.addCleanup(pair.close)
        runtime=pair.ready(**kw);runtime['observation']=pair.ctx.collect(runtime)
        return pair,runtime,row_for(pair,runtime)

    def test_completed_verified_zero_is_distinct_from_empty_intake(self):
        with tempfile.TemporaryDirectory() as tmp:
            pair,runtime,row=self.setup_pair(tmp)
            q=qualify_observation(row,runtime,pair.c)
            self.assertEqual(q['qualification'],'qualified',q)
            self.assertEqual(q['admissible_count'],0)
            row['instrumentation_observation']=q
            ledger=candidate_ledger([row])
            self.assertEqual(ledger['instrumentation_status'],'completed')
            self.assertEqual(ledger['instrumentation_candidates'],0)
            self.assertEqual(ledger['confirmed_candidates'],0)

    def test_not_run_and_unknown_count_remain_null(self):
        self.assertIsNone(qualify_observation({}, {}, {})['admissible_count'])
        with tempfile.TemporaryDirectory() as tmp:
            pair,runtime,row=self.setup_pair(tmp,missing_qos=True)
            q=qualify_observation(row,runtime,pair.c)
            self.assertEqual(q['intake_count'],0)
            self.assertIsNone(q['admissible_count'])
            self.assertFalse(q['zero_candidate'])
            row['instrumentation_observation']=q
            ledger=candidate_ledger([row])
            self.assertEqual(ledger['instrumentation_status'],'incomplete')
            self.assertIsNone(ledger['instrumentation_candidates'])

    def test_rehashed_labels_do_not_replace_raw_provenance(self):
        with tempfile.TemporaryDirectory() as tmp:
            pair,runtime,row=self.setup_pair(tmp)
            for mode in ('summary','raw','source','canonical','binary'):
                item=copy.deepcopy(runtime); changed=copy.deepcopy(row)
                if mode=='summary': item['observation']['candidate_count']=7
                if mode=='raw': changed['raw_sha256']='0'*64
                if mode=='source':item['execution']['source_after']['head']='different'
                if mode=='canonical': changed['execution_stage']='planned'
                if mode=='binary':
                    lib=next(pair.libs.iterdir()); old=lib.read_bytes(); lib.write_bytes(b'changed')
                q=qualify_observation(changed,item,pair.c)
                self.assertIsNone(q['admissible_count'],mode)
                self.assertTrue(q['errors'],mode)
                if mode=='binary':lib.write_bytes(old)

    def test_same_input_planned_task_does_not_count_as_execution(self):
        with tempfile.TemporaryDirectory() as tmp:
            pair,runtime,row=self.setup_pair(tmp)
            row['instrumentation_observation']=qualify_observation(row,runtime,pair.c)
            self.assertEqual(observation_links([row])[0]['status'],'not_run')
            other=copy.deepcopy(row);other.update(run_id='independent',domain_id=175,raw_path='different')
            other['instrumentation_observation']={'qualification':'blackbox_observed','non_tracing_profiles':copy.deepcopy(row['instrumentation_observation']['non_tracing_profiles'])}
            link=observation_links([row,other])[0]
            self.assertEqual(link['status'],'executed')
            self.assertEqual(link['candidate_confirmation'],'not_performed')
            self.assertEqual(runtime['observation']['blackbox_task']['status'],'not_run')

    def test_limited_fail_intake_is_preserved_when_core_canonical_admission_fails(self):
        with tempfile.TemporaryDirectory() as tmp:
            pair,runtime,row=self.setup_pair(tmp,durability='transient_local')
            q=qualify_observation(row,runtime,pair.c)
            self.assertEqual(q['qualification'],'qualified',q)
            self.assertEqual(q['admissible_count'],1)
            row['instrumentation_observation']=q
            row['typed_facts']={role:{'id':pair.endpoints[role]['endpoint_gid'],'generation':role+'-generation'} for role in ('writer','reader')}
            ledger=candidate_ledger([row])
            self.assertEqual(len(ledger['candidates']),1)
            self.assertEqual(ledger['candidates'][0]['candidate_source'],'instrumentation')
            self.assertTrue(ledger['candidates'][0]['admission_error'])
            self.assertEqual(ledger['confirmed_candidates'],0)


if __name__=='__main__': unittest.main()
