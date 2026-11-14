"""Fourth target is wire/graph equality; these are offline fact contracts only."""
import copy
import unittest
from r2f.modules.s1.module import S1Module
from r2f.modules.s1.targets import EXECUTION_ORDER, RULES
from r2f.modules.s1.oracles import evaluate_primary
from r2f.campaign.identity import semantic_input

FOURTH=EXECUTION_ORDER[3]
FIELDS=('endpoint_guid','role','topic','type','reliability','durability','deadline','liveliness.kind','liveliness.lease_duration')


def facts():
    comparisons=[{'endpoint_guid':guid,'generation':role+'-generation','field':name,'equal':True,'wire':{'status':'observed','value':'value'},'graph_value':'value'}
                 for role,guid in (('writer','1'*32),('reader','2'*32)) for name in FIELDS]
    return {'writer':{'id':'1'*32,'generation':'writer-generation','created_ns':30},
            'reader':{'id':'2'*32,'generation':'reader-generation','created_ns':31},
            'window':{'start_ns':10,'end_ns':40,'complete':True},
            'projection':{'comparison_status':'consistent','comparisons':comparisons,'differences':[],
                          'unknown_reasons':[],'baseline_fields':list(FIELDS),'history_depth':'not_applicable_by_endpoint_schema',
                          'independent_sources':{'wire':{'path':'wire','sha256':'a'*64},'graph':{'path':'graph','sha256':'b'*64}}},
            'diagnostics':{'actual_match':{'status':'not_observed','finite_window':True},'offered_requested':{'compatible':True}}}


class ProjectionModel(unittest.TestCase):
    def test_only_real_pre_mutation_steps(self):
        target=S1Module().targets[FOURTH]
        self.assertEqual([s.step_id for s in target.state_steps],['namespace_ready','observer_ready'])
        self.assertEqual(target.rule_contract['primary_rule'],'builtin.discovery.builtin_matches_projection')
        self.assertEqual(target.control_contract['fault']['receipt_kind'],'projection_field_mismatch')
        self.assertEqual(target.target_revision,0)

    def test_actual_qos_variant_and_neutral_payload_identity(self):
        baseline=semantic_input('baseline',target_id=FOURTH)
        self.assertEqual(baseline,semantic_input('transparent',target_id=FOURTH))
        mutated=semantic_input('mutated',target_id=FOURTH)
        self.assertNotEqual(baseline['qos'],mutated['qos'])
        self.assertEqual(mutated['qos'],mutated['reader_qos'])
        self.assertEqual(semantic_input('negative',target_id=FOURTH)['fault_id'],'controlled_projection_field_mismatch')

    def test_equality_primary_independent_of_compatibility_or_match_diagnostic(self):
        value=facts()
        result=evaluate_primary(value,FOURTH)
        self.assertEqual(result['verdict'],'pass',result)
        self.assertEqual(result['rule_id'],RULES[FOURTH])
        value['diagnostics']['offered_requested']['compatible']=False
        self.assertEqual(evaluate_primary(value,FOURTH)['verdict'],'pass')

    def test_missing_field_or_source_never_equality_pass(self):
        for mode in ('field','source','generation','incomplete','bool_equal'):
            value=facts()
            if mode=='field': value['projection']['comparisons'].pop()
            if mode=='source':value['projection']['independent_sources']['graph']=copy.deepcopy(value['projection']['independent_sources']['wire'])
            if mode=='generation':value['projection']['comparisons'][0]['generation']='different'
            if mode=='incomplete':value['window']['complete']=False
            if mode=='bool_equal':value['projection']['comparisons'][0]['equal']=1
            self.assertEqual(evaluate_primary(value,FOURTH)['verdict'],'unknown',mode)

    def test_forbidden_residual_outranks_allowed_qos_effect(self):
        value=facts(); row=value['projection']['comparisons'][4]
        row.update(equal=False,graph_value='different')
        value['projection'].update(comparison_status='mismatch',differences=[copy.deepcopy(row)])
        value['observed_effects']=['expected_qos_projection']
        self.assertEqual(evaluate_primary(value,FOURTH)['verdict'],'fail')
        value['projection']['unknown_reasons']=['capture_gap']
        self.assertEqual(evaluate_primary(value,FOURTH)['verdict'],'unknown')


if __name__=='__main__':unittest.main()
