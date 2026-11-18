"""Portable F2 evidence replays; kernel capture is explicitly mocked.

The real window scheduler emits every event in these fixtures. No runner, ROS,
network namespace, nft/tc command or campaign is executed. Packet frame auditing
remains covered by the independent recovery adapter tests, not these mocks.
"""
from copy import deepcopy
import hashlib
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from r2f.campaign import recovery_adapter as adapter
from r2f.campaign.state_projection import attach_state_observations
from r2f.core.evidence import canonical_hash
from r2f.runtime import namespace_pair as ns
from test_f2_windows_timing import Harness, configuration


def packet_proof(event, capture, tokens, since, until, ports, bindings, *, after_ns=None):
    return dict(unix_ns=event['time_ns'], received_monotonic_ns=event['monotonic_ns'])


class F2Receipts(unittest.TestCase):
    def setUp(self):
        self.directory=tempfile.TemporaryDirectory();self.addCleanup(self.directory.cleanup)
        self.packet=patch.object(adapter,'_activity',side_effect=packet_proof)
        self.packet.start();self.addCleanup(self.packet.stop)

    def fixture(self,count=2,surface='E02',zero=None,**configuration_options):
        self.h=Harness(configuration(count,surface,**configuration_options));self.h.zero_window=zero;self.h.run()
        self.runtime=self.h.finish(self.directory.name)
        return self.h

    def windows(self):
        h=self.h;facts=[];previous=h.precondition['raw_event_ref']
        for i,plan in enumerate(h.c['environment_windows']):
            fact=adapter._window_fact(h.events,i,plan,h.endpoints,h.isolation,h.c,previous,
                                     {},h.ports,h.bindings['bindings'],ns.CLOCK_GUARD_NS)
            facts.append(fact);previous=fact['recovery_event_ref']
        return facts

    def audit(self):
        return adapter._f2_operation_facts(self.h.events,self.runtime,self.h.c,{'windows':self.windows()})

    def rehash(self):
        path=Path(self.runtime['raw_path'])
        path.write_text(''.join(json.dumps(e,sort_keys=True)+'\n' for e in self.h.events))
        self.runtime['raw_sha256']=hashlib.sha256(path.read_bytes()).hexdigest()
        for receipt in self.runtime['operation_receipts']:
            for ref in receipt['raw_refs']:ref['sha256']=self.runtime['raw_sha256']

    def test_M07_and_M06_hit_only_with_actual_per_window_actions_and_raw(self):
        for surface in ('E02','E03'):
            for count in (1,2,3):
                with self.subTest(surface=surface,count=count):
                    h=self.fixture(count,surface);ops=self.audit()
                    self.assertEqual(len(ops),1);op=ops[0]
                    self.assertTrue(op['verified']);self.assertTrue(op['hit'])
                    self.assertEqual(op['mutator_id'],'M07' if surface=='E02' else 'M06')
                    self.assertEqual(op['actual_window_count'],count)
                    self.assertEqual(len(op['actual_restore_intervals']),count-1)
                    self.assertEqual(op['trailing_interval']['status'],'not_applicable')
                    self.assertEqual(len(h.installs),count);self.assertEqual(len(h.restores),count)
                    self.assertEqual(len([e for e in h.events if e['event']=='discovery_drop_begin']),count)
                    for ref in op['raw_refs']:
                        self.assertEqual(ref['sha256'],hashlib.sha256(Path(ref['path']).read_bytes()).hexdigest())
                        self.assertEqual(h.events[ref['event_index']]['event_index'],ref['event_index'])

    def test_zero_counter_cannot_borrow_hit_from_earlier_window_or_aggregate(self):
        self.fixture(3,zero=1)
        self.runtime['mutation_receipt']={'hit':True,'effect_observed':True}
        op=self.audit()[0]
        self.assertFalse(op['hit']);self.assertFalse(op['effect_observed'])
        self.assertEqual([w['hit'] for w in op['windows']],[True,False,True])

    def test_fake_counter_summary_or_fake_nft_counter_is_rejected(self):
        for mode in ('summary','nft','command','delta'):
            with self.subTest(mode=mode):
                h=self.fixture(1)
                drop=next(e for e in h.events if e['event']=='discovery_drop_end')
                snapshot=drop['receipt']['counter_snapshot']
                if mode=='summary':snapshot['packets']=99
                if mode=='nft':snapshot['nft_json']['nftables'][0]['counter']['packets']=99
                if mode=='command':snapshot['command']['net_namespace']='net:[foreign]'
                if mode=='delta':next(e for e in h.events if e['event']=='discovery_window_counter')['hit']=False
                with self.assertRaises((ValueError,RuntimeError)):self.windows()

    def test_adapter_recomputes_offset_instead_of_echoing_configuration(self):
        h=self.fixture(1,'E03');timing=next(e for e in h.events if e['event']=='discovery_window_timing')
        timing['earliest_due_monotonic_ns']-=10
        with self.assertRaisesRegex(ValueError,'f2_timing_receipt_mismatch'):self.windows()

    def test_early_late_or_forged_install_cannot_become_sut_fail(self):
        for mode in ('early','late','forged_install','future_anchor'):
            with self.subTest(mode=mode):
                h=self.fixture(1);timing=next(e for e in h.events if e['event']=='discovery_window_timing')
                drop=next(e for e in h.events if e['event']=='discovery_drop_begin')
                if mode=='early':timing['actual_start_monotonic_ns']=timing['earliest_due_monotonic_ns']-1
                if mode=='late':timing['overshoot_ns']=600_000_000
                if mode=='forged_install':drop['receipt']['drop_applied_monotonic_ns']+=600_000_000
                if mode=='future_anchor':h.precondition['monotonic_ns']=timing['actual_install_monotonic_ns']+1
                with self.assertRaises(ValueError):self.windows()

    def test_wrong_window_anchor_identity_generation_and_stale_ACK_rejected(self):
        for mode in ('window','anchor','pid','generation','old_ack','unrestored'):
            with self.subTest(mode=mode):
                h=self.fixture(2)
                if mode=='window':next(e for e in h.events if e['event']=='discovery_window_timing')['window_id']='other'
                if mode=='anchor':next(e for e in h.events if e['event']=='discovery_window_begin' and e['window_index']==1)['anchor_ref']=h.precondition['raw_event_ref']
                if mode=='pid':next(e for e in h.events if e['event']=='discovery_drop_begin')['receipt']['pair_bindings']['worker_pids']['reader']=999
                if mode=='generation':next(e for e in h.events if e['event']=='command_done')['endpoint_generation']='old'
                if mode=='old_ack':next(e for e in h.events if e['event']=='command_done')['command_id']=-1
                if mode=='unrestored':next(e for e in h.events if e['event']=='discovery_recovery_confirmed')['receipt']['recovery_verified']=False
                with self.assertRaises(ValueError):self.windows()

    def test_plan_operation_and_raw_ref_cannot_be_swapped(self):
        for mode in ('op_id','surface','mutator','plan_hash','ref','count','aggregate'):
            with self.subTest(mode=mode):
                h=self.fixture(2)
                raw=next(e for e in h.events if e['event']=='f2_operation_receipt')['receipt']
                if mode=='op_id':raw['op_id']='another'
                if mode=='surface':raw['surface_id']='E01'
                if mode=='mutator':raw['mutator_id']='M03'
                if mode=='plan_hash':raw['plan_hash']='0'*64
                if mode=='ref':self.runtime['operation_receipts'][0]['raw_refs'][0]['event_index']=0
                if mode=='count':raw['window_count']=1
                if mode=='aggregate':raw['windows'][0]['counter_ref']=raw['windows'][1]['counter_ref']
                self.rehash()
                with self.assertRaises(ValueError):self.audit()

    def test_missing_raw_or_summary_only_receipt_is_not_execution(self):
        for mode in ('raw','operation','event'):
            with self.subTest(mode=mode):
                h=self.fixture(1)
                if mode=='raw':Path(self.runtime['raw_path']).unlink()
                if mode=='operation':self.runtime['operation_receipts']=[]
                if mode=='event':h.events=[e for e in h.events if e['event']!='f2_operation_receipt'];self.rehash()
                with self.assertRaises((ValueError,OSError)):self.audit()

    def test_wrong_window_count_interval_offset_and_plan_hash_rejected_at_admission(self):
        for surface,field,value in [('E02','window_count',3),('E02','restore_interval_sec',0.9),
                                    ('E03','offset_sec',0.9),('E03','anchor','previous_restore_confirmed')]:
            h=self.fixture(2,surface)
            h.c['mutation_plan']['operations'][0]['parameters'][field]=value
            h.c['mutation_plan_sha256']=canonical_hash(h.c['mutation_plan'])
            with self.subTest(surface=surface,field=field),self.assertRaises(ValueError):self.audit()

    def test_projection_requires_verified_operation_not_an_aggregate_hit(self):
        fixtures=json.loads((Path(__file__).parent/'fixtures'/'state_projection_receipts.json').read_text())['fixtures']
        fixture=deepcopy(fixtures['recovery-multi-judged'])
        facts,events,runtime,config=[fixture[k] for k in ('facts','events','runtime','config')]
        config['target_id']='discovery.interruption_recovery.fuzz_windows_v1'
        runtime['raw_path']=str(Path(self.directory.name)/'projection.jsonl')
        payload=''.join(json.dumps(e,sort_keys=True)+'\n' for e in events).encode()
        Path(runtime['raw_path']).write_bytes(payload);runtime['raw_sha256']=hashlib.sha256(payload).hexdigest()
        facts.setdefault('drop',{})['hit']=True
        result=attach_state_observations(facts,events,runtime,config)
        self.assertFalse(result['observation_complete'])
        self.assertNotEqual(result['state_projection_receipt']['status'],'observed')

    def test_full_recovery_adapter_wires_operation_receipts_and_harness_errors(self):
        # Only the OS packet-capture reader is mocked. Profile hashes, role QoS,
        # commands/counters, matched snapshots, timing and operations are real auditors.
        from r2f.runtime.discovery_profiles import CYCLONE_XML
        h=self.fixture(2)
        root=Path(self.directory.name);profiles={}
        for role in ('writer','reader'):
            path=root/(role+'.xml');path.write_text(CYCLONE_XML)
            profiles[role]=dict(path=str(path),sha256=hashlib.sha256(path.read_bytes()).hexdigest())
        self.runtime.update(environment={'ok':True,'discovery_namespace':{'profiles':profiles}},
            execution_metadata={'workers':[dict(role=r,pid=t['pid'],returncode=0) for r,t in h.endpoints.items()]},
            carrier_implementation=adapter.IMPLEMENTATION,precondition_receipts=[{}, {}, {}],observation_receipts=[])
        # Missing state-only fixture scaffolding must not conceal the adapter result.
        with patch.object(adapter,'_capture',return_value={}):
            facts=adapter.recovery_facts(h.events,self.runtime,h.c)
            self.assertNotIn('evidence_error',facts)
            self.assertEqual(len(facts['operation_receipts']),1)
            self.assertTrue(facts['operation_receipts'][0]['verified'])
            self.assertEqual(facts['state_projection_receipt']['status'],'observed',facts['state_projection_receipt']['reasons'])
            next(e for e in h.events if e['event']=='discovery_window_timing')['overshoot_ns']=900_000_000
            facts=adapter.recovery_facts(h.events,self.runtime,h.c)
            self.assertFalse(facts['observation_complete'])
            self.assertFalse(facts['candidate_eligible'])
            self.assertEqual(facts['responsibility_layer'],'harness')
            self.assertIn('timing_receipt_mismatch',facts['evidence_error'])


    def test_previous_restore_operation_receipt_has_exact_affected_set_and_raw_offsets(self):
        for count in (2,3):
            h=self.fixture(count,'E03',offsets=[0.0]+[0.4]*(count-1),anchor='previous_restore_confirmed')
            op=self.audit()[0]
            self.assertEqual(op['affected_window_indices'],list(range(1,count)))
            actual=op['observed_parameters']['actual_offsets']
            self.assertEqual([v['window_index'] for v in actual],list(range(1,count)))
            self.assertTrue(all(v['actual_offset_sec']>=0.4 for v in actual))
            self.assertEqual(h.events[0]['event'],'environment_preflight')
            raw=next(e for e in h.events if e['event']=='f2_operation_receipt')['receipt']
            raw['affected_window_indices']=[0];self.rehash()
            with self.assertRaisesRegex(ValueError,'affected_windows'):self.audit()

    def test_nonexecuted_or_unclosed_runtime_never_gets_operation_receipts(self):
        for key,value in [('real_execution',False),('runtime_status','blocked')]:
            self.fixture(1);self.runtime[key]=value
            with self.subTest(key=key),self.assertRaisesRegex(ValueError,'not_completed_execution'):self.audit()
