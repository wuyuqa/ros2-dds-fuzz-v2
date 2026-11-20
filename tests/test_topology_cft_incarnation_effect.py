"""Synthetic API identity contracts: not DDS execution or filtering proof."""
from copy import deepcopy
import pytest
from test_endpoint_incarnation import trace, OLD, NEW
from r2f.runtime.topology_pair import _dynamic_cft_effect


def api_trace():
    rows=trace()
    requested=dict(enabled=True,expression='data LIKE %0',parameters=["'%x%'"])
    observed=dict(expression=requested['expression'],parameters=requested['parameters'])
    raw=dict(type='rclpy.subscription_content_filter_options.ContentFilterOptions',
        status='captured',shape='namedtuple',fields=dict(filter_expression=observed['expression'],
        expression_parameters=observed['parameters']),parameters_type='builtins.list')
    rows[2]['requested']=requested
    template=deepcopy(rows[3])
    rows[4:4]=[dict(template,event='content_filter_api_call',api_stage='get_begin'),
        dict(template,event='content_filter_api_call',api_stage='get_success',observed=observed,raw_readback=raw)]
    receipt=dict(api_status='succeeded',readback_status='matched',applied=True,
                 requested=requested,observed=observed)
    rows[7]['filter_receipt']=receipt
    rows.append(dict(rows[0],event='topology_filter_update_result',command_id=7,
        reader_gid=NEW,receipt=receipt))
    for i,r in enumerate(rows):
        r.update(event_index=i,monotonic_ns=100+i)
        if r['event']=='content_filter_api_call':r['api_monotonic_ns']=r['monotonic_ns']
    rows[7].update(update_started_ns=101,update_finished_ns=107)
    return rows


def test_valid_transition_passes_api_gate_but_not_missing_window():
    rows=api_trace()
    value=_dynamic_cft_effect(rows[-1],rows,real_ros=True)
    assert value['status']=='unknown' and value['filter_effect_observed'] is None
    assert value['reason']=='pre_or_post_raw_window_missing',value
    assert value['candidate_eligible'] is False


@pytest.mark.parametrize('fault',['no_transition','no_ack','wrong_ack','old_after','wrong_before',
                                 'wrong_worker','wrong_graph','late_ack','downgrade','wrong_command'])
def test_changed_gid_needs_complete_current_command_proof(fault):
    rows=api_trace()
    if fault=='no_transition':rows[8]['event']='removed'
    elif fault=='no_ack':rows[9]['event']='removed'
    elif fault=='wrong_ack':rows[9]['reader_gid']=OLD
    elif fault=='old_after':rows[7]['after_gid']=OLD
    elif fault=='wrong_before':rows[2]['reader_gid']=NEW
    elif fault=='wrong_worker':rows[6]['pid']=999
    elif fault=='wrong_graph':rows[6]['subscriptions'][0]['endpoint_gid']=OLD
    elif fault=='late_ack':rows[9]['monotonic_ns']=1000
    elif fault=='downgrade':
        for row in rows:
            if row['event'] in ('endpoint_identity_snapshot','endpoint_identity_transition'):row['event']='removed'
        rows[7].pop('before_gid');rows[7].pop('after_gid')
    else:rows[8]['command_id']=8
    value=_dynamic_cft_effect(rows[-1],rows,real_ros=True)
    assert value['status']=='unknown' and value['filter_effect_observed'] is None
    assert value['reason']!='pre_or_post_raw_window_missing',value


def test_readback_failure_not_promoted_by_identity_transition():
    rows=api_trace()
    rows[-1]['receipt']['readback_status']='failed'
    assert _dynamic_cft_effect(rows[-1],rows,real_ros=True)['reason']=='update_api_or_actual_readback_unavailable'


def test_observed_api_identity_receipt_does_not_claim_delivery_effect():
    rows=api_trace();value=_dynamic_cft_effect(rows[-1],rows,real_ros=True)
    assert value['api_identity_observed'] and value['configuration_readback_observed']
    assert value['reader_identity']==dict(before_gid=OLD,after_gid=NEW,
        incarnation_before=0,incarnation_after=1,transition_ref=8,ack_ref=9)
    assert value['api_call_refs']==[2,3,4,5]
    assert value['status']=='unknown' and value['filter_effect_observed'] is None
    rows[9]['reader_gid']=OLD
    rejected=_dynamic_cft_effect(rows[-1],rows,real_ros=True)
    assert not rejected.get('api_identity_observed',False)


def test_new_contract_unchanged_gid_uses_ack_without_faking_transition():
    rows=api_trace()
    rows[8]['event']='removed'
    for row in rows:
        for key in ('endpoint_gid','reader_gid','after_gid'):
            if row.get(key)==NEW:row[key]=OLD
        for member in row.get('subscriptions',[]):
            if member.get('endpoint_gid')==NEW:member['endpoint_gid']=OLD
    rows[9]['incarnation_ordinal']=0
    value=_dynamic_cft_effect(rows[-1],rows,real_ros=True)
    assert value['reason']=='pre_or_post_raw_window_missing',value
    assert value['reader_identity']['before_gid']==value['reader_identity']['after_gid']==OLD
    assert value['reader_identity']['transition_ref'] is None
    assert value['reader_identity']['ack_ref']==9


def test_api_identity_accepts_raw_reader_alias_without_rewriting_it():
    rows=api_trace()
    for row in rows:row['role']='reader2'
    value=_dynamic_cft_effect(rows[-1],rows,real_ros=True)
    assert value['api_identity_observed'] and value['reason']=='pre_or_post_raw_window_missing'
    assert rows[0]['role']=='reader2'
