"""Synthetic API/graph lineage contracts, not evidence of DDS execution."""
from copy import deepcopy
import pytest
from r2f.runtime.endpoint_incarnation import endpoint_identity_at, validate_filter_identity_update

OLD, NEW, NEXT = '01' * 16, '02' * 16, '03' * 16

def trace():
    base = dict(logical_id='r0', generation=1, pid=100, pgid=100,
                process_start_ticks=5, endpoint_generation='worker-a', run_id='run',
                domain_id=191, monotonic_clock_identity='boot/time', node_name='reader_node',
                rmw_identifier='rmw_fastrtps_cpp', role='reader')
    def row(event, **fields):
        return dict(base, event=event, **fields)
    def graph(gid):
        return dict(publishers=[], subscriptions=[dict(node_name='reader_node',
                      endpoint_gid=gid, topic_type='std_msgs/msg/String')])
    origin = row('endpoint_created', endpoint_gid=OLD, topic='/t', message_type='std_msgs/msg/String')
    c = dict(command_id=7)
    rows = [origin,
        row('endpoint_identity_snapshot', **c, command='update_filter', phase='before',
            topic='/t', endpoint_gid=OLD, identity_source='get_subscriptions_info_by_topic', **graph(OLD)),
        row('content_filter_api_call', **c, reader_gid=OLD, api_stage='set_begin',
            requested=dict(expression='data LIKE %0', parameters=["'%x%'"])),
        row('content_filter_api_call', **c, reader_gid=OLD, api_stage='set_success'),
        row('endpoint_identity_snapshot', **c, command='update_filter', phase='after',
            topic='/t', endpoint_gid=NEW, identity_source='get_subscriptions_info_by_topic', **graph(NEW)),
        row('content_filter_dynamic_update', **c, reader_gid=NEW, before_gid=OLD, after_gid=NEW,
            filter_receipt=dict(api_status='succeeded', applied=True)),
        row('endpoint_identity_transition', **c, command='update_filter',
            schema='r2f.endpoint-identity-transition/v1', before_gid=OLD, after_gid=NEW,
            endpoint_gid=NEW, incarnation_before=0, incarnation_after=1,
            api='rclpy.subscription.Subscription.set_content_filter',
            identity_source='get_subscriptions_info_by_topic'),
        row('command_done', **c, command='update_filter', endpoint_gid=NEW,
            reader_gid=NEW, incarnation_ordinal=1)]
    for i, r in enumerate(rows):
        r.update(event_index=i, monotonic_ns=100 + i, time_ns=200 + i)
    rows[5].update(update_started_ns=101, update_finished_ns=105)
    for r in rows:
        if r['event']=='content_filter_api_call':
            r['api_monotonic_ns']=r['monotonic_ns']
    return rows


def test_switch_preserves_origin_and_prefix_identity():
    rows=trace(); original=deepcopy(rows)
    current=endpoint_identity_at(rows, rows[0])
    assert current['endpoint_gid']==NEW and current['incarnation_ordinal']==1
    assert current['identity_transition_ref']==6
    assert current['event_index']==0 and current['endpoint_generation']=='worker-a'
    assert endpoint_identity_at(rows, rows[0], before_index=1)['endpoint_gid']==OLD
    assert rows==original
    assert validate_filter_identity_update(rows, rows[0], 7)['endpoint_gid']==NEW


@pytest.mark.parametrize('fault', ['no_before','no_after','no_ack','no_set','duplicate',
    'wrong_worker','wrong_run','wrong_domain','wrong_clock','wrong_old','wrong_ordinal',
    'wrong_graph','ambiguous_graph','old_type','order','zero_gid','alias','wrong_command'])
def test_transition_requires_complete_independent_raw_chain(fault):
    rows=trace()
    if fault.startswith('no_'):
        index={'no_before':1,'no_after':4,'no_ack':7,'no_set':3}[fault]
        rows.pop(index)
    elif fault=='duplicate': rows.append(deepcopy(rows[6]))
    elif fault=='wrong_worker': rows[4]['pid']=101
    elif fault=='wrong_run': rows[4]['run_id']='other'
    elif fault=='wrong_domain': rows[4]['domain_id']=192
    elif fault=='wrong_clock': rows[4]['monotonic_clock_identity']='other'
    elif fault=='wrong_old': rows[6]['before_gid']=NEXT
    elif fault=='wrong_ordinal': rows[6]['incarnation_after']=2
    elif fault=='wrong_graph': rows[4]['subscriptions'][0]['endpoint_gid']=OLD
    elif fault=='ambiguous_graph': rows[4]['subscriptions'].append(deepcopy(rows[4]['subscriptions'][0]))
    elif fault=='old_type': rows[4]['subscriptions'][0]['topic_type']='other/Type'
    elif fault=='order': rows[4]['monotonic_ns']=101
    elif fault=='zero_gid': rows[6]['after_gid']='00'*16
    elif fault=='alias':
        other=dict(rows[0], logical_id='r1', pid=101, endpoint_generation='worker-b', endpoint_gid=NEW)
        rows.insert(1,other)
    else: rows[4]['command_id']=8
    with pytest.raises(ValueError): endpoint_identity_at(rows, rows[0])


def test_unreceipted_gid_cannot_change_identity():
    rows=trace()[:1]
    rows.append(dict(rows[0], event='match_snapshot', endpoint_gid=NEW, event_index=1, monotonic_ns=101))
    assert endpoint_identity_at(rows, rows[0])['endpoint_gid']==OLD


def test_transition_cannot_be_used_before_command_ack():
    rows=trace()
    with pytest.raises(ValueError):
        endpoint_identity_at(rows, rows[0], before_index=7)


def test_unchanged_update_is_not_an_incarnation():
    rows=trace()
    rows.pop(6)
    for r in rows:
        for key in ('endpoint_gid','reader_gid','after_gid'):
            if r.get(key)==NEW: r[key]=OLD
    rows[4]['subscriptions'][0]['endpoint_gid']=OLD
    rows[-1]['incarnation_ordinal']=0
    assert validate_filter_identity_update(rows,rows[0],7)['endpoint_gid']==OLD
    assert validate_filter_identity_update(rows,rows[0],7)['incarnation_ordinal']==0


def test_two_updates_bind_each_incarnation_without_rewriting_creation():
    rows=trace()
    second=deepcopy(rows[1:])
    for r in second:
        r['command_id']=9
        for key in ('endpoint_gid','reader_gid','before_gid','after_gid'):
            if key in r: r[key]={OLD:NEW,NEW:NEXT}[r[key]]
        for member in r.get('subscriptions',[]):
            member['endpoint_gid']={OLD:NEW,NEW:NEXT}[member['endpoint_gid']]
        if r['event']=='endpoint_identity_transition':
            r.update(incarnation_before=1,incarnation_after=2)
        if r['event']=='command_done': r['incarnation_ordinal']=2
    rows.extend(second)
    for i,r in enumerate(rows):
        r.update(event_index=i,monotonic_ns=100+i)
        if r['event']=='content_filter_api_call':r['api_monotonic_ns']=r['monotonic_ns']
    rows[12].update(update_started_ns=108,update_finished_ns=112)
    assert endpoint_identity_at(rows,rows[0],before_index=8)['endpoint_gid']==NEW
    current=endpoint_identity_at(rows,rows[0])
    assert current['endpoint_gid']==NEXT and current['incarnation_ordinal']==2
    assert current['event_index']==0


def test_identity_graph_observation_is_not_a_filter_success_claim():
    rows=trace()
    rows[3]['api_stage']='set_failure'
    rows[5]['filter_receipt'].update(api_status='failed',applied=False)
    current=endpoint_identity_at(rows,rows[0])
    assert current['endpoint_gid']==NEW
    assert rows[5]['filter_receipt']['applied'] is False


@pytest.mark.parametrize('field,value', [('command','snapshot'),('endpoint_gid',OLD),
    ('reader_gid',OLD),('incarnation_ordinal',0),('command',None)])
def test_ack_must_ack_this_update_and_new_identity(field,value):
    rows=trace();rows[-1][field]=value
    with pytest.raises(ValueError):endpoint_identity_at(rows,rows[0])


def test_public_worker_reader_alias_retains_raw_role():
    rows=trace()
    for row in rows:row['role']='reader2'
    current=endpoint_identity_at(rows,rows[0])
    assert current['role']=='reader2' and current['endpoint_gid']==NEW


def test_api_clock_cannot_precede_this_update_snapshot():
    rows=trace();rows[2]['api_monotonic_ns']=1
    with pytest.raises(ValueError):endpoint_identity_at(rows,rows[0])


def test_second_update_cannot_precede_first_ack_time():
    rows=trace();second=deepcopy(rows[1:])
    for row in second:
        row['command_id']=9
        for key in ('endpoint_gid','reader_gid','before_gid','after_gid'):
            if key in row:row[key]={OLD:NEW,NEW:NEXT}[row[key]]
        for member in row.get('subscriptions',[]):
            member['endpoint_gid']={OLD:NEW,NEW:NEXT}[member['endpoint_gid']]
        if row['event']=='endpoint_identity_transition':row.update(incarnation_before=1,incarnation_after=2)
        if row['event']=='command_done':row['incarnation_ordinal']=2
        row['event_index']+=7  # capture order advances; clock illegally rewinds.
    rows.extend(second)
    with pytest.raises(ValueError):endpoint_identity_at(rows,rows[0])
