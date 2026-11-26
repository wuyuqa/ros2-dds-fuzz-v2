"""Source-bound supporting lookup predicate; mutation/input contracts are unchanged."""
RULE = 's1.topology.topic_lookup.v1'
RESIDUAL = 'topology.topic_lookup_mismatch'


def predicate_contract():
    return {'schema':'r2f.topic-lookup-contract/v1','rule_id':RULE,'rule_role':'supporting',
            'verdict_rule_id':'s1.topology.program_consistency.v1','spec_refs':['S1#6:C09/T08','S1#22'],
            'forbidden_residuals':[RESIDUAL],'input_contract_changed':False,
            'predicate_scope':'qualified Cyclone C typed scopes; Fast DDS participant-local name and type readback',
            'evidence_scope':'current endpoint/generation, raw worker result and ACK, stable ownership, released lookup resources',
            'missing_policy':'unknown; unsupported and adapter errors never prove a SUT mismatch',
            'binding_scope':'native API observation; no historical or cross-binding reproduction claim'}
