"""CP2 bounded public input/carrier/artifact contracts, strictly offline fixtures."""
from copy import deepcopy

from r2f.modules.s1 import topology_model as model
from r2f.runtime.participant_groups import CONTROL
from test_s1_topology_model import action, endpoint, program


def multi_program(writers=1, readers=2, layout='shared'):
    eps = [endpoint(f'w{i}', 'writer') for i in range(writers)]
    eps += [endpoint(f'r{i}', 'reader') for i in range(readers)]
    for ep in eps:
        ep['participant_id'] = ('all' if layout == 'shared' else
                                ep['role'] if layout == 'roles' else ep['logical_id'])
    p = program(eps)
    p['participant_control'] = dict(CONTROL)
    return p


def test_1w2r_input_enters_existing_plan_and_receipt_without_execution_authority():
    from r2f.modules.s1.topology_input import make_topology_plan, validate_topology_plan
    from r2f.runtime.topology_pair import build_input_receipts
    p = multi_program()
    assert model.validate_program(p)['status'] == 'valid'
    plan = make_topology_plan(p, [{'operator_id': 'action.insert'}], rng_seed=7)
    validate_topology_plan(plan)
    final = plan.final_config['topology_program']
    receipt = build_input_receipts(final, {'rmw': 'rmw_cyclonedds_cpp'})
    assert [e['logical_id'] for e in final['endpoints']] == ['w0', 'r0', 'r1']
    assert receipt['participant_control'] == CONTROL
    assert not receipt['execution_evidence'] and not receipt['candidate_eligible']


def worker_capture(monkeypatch, writers=1, readers=2):
    """Use the established external ROS/OS fixture, with distinct observed GIDs."""
    import json
    import sys
    from r2f.runtime import topology_worker as worker
    from test_topology_participant_control_worker import ros_fixture, group_config
    p = multi_program(writers, readers)
    cfg = group_config()
    cfg.update(group_id='all', group_members=p['endpoints'])
    commands = [dict(command='create_endpoint', command_id=i, logical_id=e['logical_id'],
                     generation=1, worker_role=e['role'])
                for i, e in enumerate(p['endpoints'], 1)]
    commands.append(dict(command='stop', command_id=len(commands)+1))
    with ros_fixture(monkeypatch, commands) as (calls, entities, stream):
        Node = sys.modules['rclpy.node'].Node
        for method in ('create_publisher', 'create_subscription'):
            original = getattr(Node, method)
            def create(self, *args, _create=original, **kwargs):
                e = _create(self, *args, **kwargs)
                e.gid = f'{len(entities):032x}'
                return e
            monkeypatch.setattr(Node, method, create)
        monkeypatch.setattr(sys, 'argv', ['worker', json.dumps(cfg), 'writer'])
        rc = worker.worker_main()
    return rc, calls, [json.loads(line) for line in stream.getvalue().splitlines()]


def test_1w2r_carrier_creates_distinct_endpoints_in_one_context(monkeypatch):
    rc, calls, events = worker_capture(monkeypatch)
    assert rc == 0, events[-1]
    created = [e for e in events if e['event'] == 'endpoint_created']
    assert [e['logical_id'] for e in created] == ['w0', 'r0', 'r1']
    assert len({e['endpoint_gid'] for e in created}) == 3
    assert calls.count('context') == calls.count('shutdown') == 1
    assert calls.count('destroy_reader') == 2


import hashlib
import json
from pathlib import Path
import pytest
from r2f.core import Scenario
from r2f.core.evidence import canonical_hash, effective_input_hash, INPUT_SCHEMA_V2
from r2f.modules.s1.topology_input import make_topology_plan, topology_input_layer_receipts
from r2f.modules.s1.topology_targets import topology_target_declaration, TOPOLOGY_TARGET, TOPOLOGY_CAPABILITY
from r2f.campaign.topology_adapter import TopologyFactProducer
from r2f.runtime.participant_groups import CONTROL, OBSERVATION_SCHEMA, project_lineage
from test_topology_adapter import RawFixture


from test_topology_participant_control_public import facts, resave


def multi_fixture(root, writers=1, readers=2, layout='shared', **kwargs):
    """Adapt the established raw protocol fixture, preserving all action evidence.

    Only supporting captures are unavailable. These rows are NOT measurements.
    """
    f=RawFixture(writers=writers, readers=readers, **kwargs)
    parent=deepcopy(f.plan.operations[0].parameters['input_layer_receipts'][0]['input_before'])
    parent['participant_control']=dict(CONTROL)
    for e in parent['endpoints']:
        e['participant_id'] = 'all' if layout == 'shared' else e['role'] if layout == 'roles' else e['logical_id']
    f.plan=make_topology_plan(parent,[{'operator_id':'action.insert'}],rng_seed=7)
    f.program=f.plan.final_config['topology_program']
    f.scenario=Scenario(topology_target_declaration(),'rmw_cyclonedds_cpp','mutated',0,seed=7,mutation_plan=f.plan)
    f.layers=topology_input_layer_receipts(f.plan)
    eff=f.scenario.effective_input_v2(final_input=f.plan.final_config,input_layer_receipts=f.layers,
        source_manifest={'source_repo':'/nonexistent/offline/fixture','head':None,'source_frozen':False})
    f.config.update(**deepcopy(eff),effective_input=eff,effective_input_sha256=effective_input_hash(eff,schema=INPUT_SCHEMA_V2),
                    plan_hash=f.plan.plan_hash(),topology_program=deepcopy(f.program))
    f.route.update(plan_hash=f.plan.plan_hash(),final_input_sha256=canonical_hash(f.plan.final_config),
                   input_layer_receipts_sha256=canonical_hash(f.layers),effective_input_sha256=f.config['effective_input_sha256'])
    f.events[0]['receipt'].update(program_sha256=canonical_hash(f.program),participant_control=dict(CONTROL),route_binding=deepcopy(f.route))
    for e in f.events:
        if e['event']=='mutation_begin': e['route_binding']=deepcopy(f.route)
    labels={e['logical_id']:e['participant_id'] for e in f.program['endpoints']}
    group_ids=list(dict.fromkeys(labels.values()))
    identities={g:dict(group_id=g,worker_instance_id='fixture-process-'+g,context_instance_id='fixture-context-'+g,
                pid=1200+group_ids.index(g),pgid=1200+group_ids.index(g),process_start_ticks=9100+group_ids.index(g)) for g in group_ids}
    serial={g:0 for g in group_ids}
    tokens={e['logical_id']:e for e in f.events if e['event']=='endpoint_created'}
    def update(value):
        if isinstance(value,dict):
            eid=value.get('logical_id')
            if eid in labels:
                # Endpoint declarations are not changed into observations.
                if 'pid' in value or 'endpoint_generation' in value:
                    value.update(identities[labels[eid]])
            for v in list(value.values()): update(v)
        elif isinstance(value,list):
            for v in value:update(v)
    update(f.events)
    for eid,t in tokens.items():
        t.update(worker_role=t['role'],rmw_identifier='rmw_cyclonedds_cpp',observed_domain_id=191,
                 domain_observation_source='offline_fixture',**identities[labels[eid]])
    additions=[]
    def row(kind,g,**kw):
        return dict(event=kind,fixture=True,backend='offline_fake',real_ros=False,run_id='fixture-topology',
                    domain_id=191,observed_domain_id=191,rmw_identifier='rmw_cyclonedds_cpp',**identities[g],**kw)
    started=set()
    pending={}
    def request(g,command,eid=None,**kw):
        serial[g]+=1
        address={} if eid is None else {k:tokens[eid][k] for k in ('logical_id','generation','endpoint_generation')}
        if command=='create_endpoint': address.pop('endpoint_generation')
        return row('group_command_requested',g,command=command,command_id=serial[g],**address,**kw)
    # Existing snapshot fixture uses the same controller CID for both endpoints;
    # group transport has process-serial addressed CIDs instead.
    for old in f.events:
        kind=old['event'];eid=old.get('logical_id');g=labels.get(eid)
        if kind=='endpoint_created':
            if g not in started:
                additions += [row('process_started',g,fresh_process=True,separate_context=False),
                    row('group_context_created',g,ros_context_created=True,api='rclpy.context.Context+rclpy.init'),
                    row('group_worker_ready',g)]
                started.add(g)
            req=request(g,'create_endpoint',eid);additions.append(req)
            additions += [old,row('endpoint_group_bound',g,endpoint={k:tokens[eid][k] for k in ('logical_id','generation','endpoint_generation','endpoint_gid')})]
            additions.append(row('command_done',g,command='create_endpoint',command_id=req['command_id'],
                                 **{k:tokens[eid][k] for k in ('logical_id','generation','endpoint_generation','endpoint_gid')}))
            continue
        if kind=='worker_command_requested' or kind=='topology_snapshot_requested':
            if g is not None:
                req=request(g,old.get('command','snapshot'),eid)
                pending[eid]=req['command_id'];additions.append(req)
        if eid is not None and 'command_id' in old and eid in pending:
            old['command_id']=pending[eid]
        if kind=='topology_cleanup':
            # Final cleanup is one actual process exit per group, with every endpoint linked.
            for label in group_ids:
                group_tokens=[t for eid,t in tokens.items() if labels[eid]==label]
                # Unknown capture is honest; no declared label->root inference.
                req=request(label,'group_snapshot',group_tokens[0]['logical_id']);additions.append(req)
                obs=dict(schema=OBSERVATION_SCHEMA,requested_grouping=label,supporting_diagnostic=True,candidate_eligible=False,
                    ros_context_application_observed=True,endpoint_ownership_observed=False,local_root_inventory_complete=False,
                    group_relation_observed=False,actual_local_root_count=None,roots_before=None,roots_after=None,endpoints=[],
                    unknown_reasons=['local_root_inventory_incomplete'])
                additions.append(row('participant_group_snapshot',label,command_id=req['command_id'],**obs))
                additions.append(row('command_done',label,command='group_snapshot',command_id=req['command_id'],
                    **{k:group_tokens[0][k] for k in ('logical_id','generation','endpoint_generation')}))
                stop=request(label,'stop');additions += [stop,row('command_done',label,command='stop',command_id=stop['command_id'])]
                for t in group_tokens:
                    additions.append(row('endpoint_destroyed',label,cleanup_status='closed',
                        **{k:t[k] for k in ('logical_id','generation','endpoint_generation','endpoint_gid')}))
                additions += [row('group_context_shutdown',label,closed=True,errors=[]),row('process_exit',label,returncode=0)]
        additions.append(old)
    mapping={id(e):i for i,e in enumerate(additions)}
    old_indices={e['event_index']:mapping[id(e)] for e in f.events}
    def refs(v):
        if isinstance(v,dict):
            for k,x in v.items():
                if (k.endswith('_ref') or k.endswith('_index')) and k not in ('action_index','event_index') and type(x) is int and x in old_indices:
                    v[k]=old_indices[x]
                elif isinstance(x,(dict,list)):refs(x)
        elif isinstance(v,list):
            for x in v:refs(x)
    refs(additions)
    for i,e in enumerate(additions):e.update(event_index=i,monotonic_ns=1000+i*1000,time_ns=1700000000000000000+i*1000)
    f.events=additions
    root=Path(root);root.mkdir(parents=True,exist_ok=True)
    f.save(root)
    lineage=project_lineage(f.program,f.events,str(f.raw),config=f.config)
    f.runtime['execution_metadata']={'schema':'r2f.topology-execution-metadata/v2','participant_groups':lineage,'workers':lineage['workers']}
    return f



def test_1w2r_every_edge_reaches_existing_facts_and_projection(tmp_path):
    f = multi_fixture(tmp_path)
    observed = facts(f)
    assert observed['diagnostic_valid'], observed
    pairs = observed['state_after']['pairs']
    assert [(e['writer'], e['reader'], e['observed_match']) for e in pairs] == [
        ('w0', 'r0', 'bidirectional'), ('w0', 'r1', 'bidirectional')]
    assert not observed['candidate_eligible'] and not observed['execution_evidence']


import pytest


@pytest.mark.parametrize('layout', ['shared', 'roles', 'split'])
def test_2w2r_four_identity_directed_edges_use_existing_report(tmp_path, layout):
    f = multi_fixture(tmp_path, writers=2, readers=2, layout=layout)
    result = facts(f)
    assert result['diagnostic_valid'], result
    assert [(p['writer'], p['reader'], p['observed_match']) for p in result['state_after']['pairs']] == [
        ('w0', 'r0', 'bidirectional'), ('w0', 'r1', 'bidirectional'),
        ('w1', 'r0', 'bidirectional'), ('w1', 'r1', 'bidirectional')]
    assert not result['candidate_eligible'] and not result['seal_eligible']


def test_2w2r_shared_carrier_uses_four_distinct_graph_identities(monkeypatch):
    rc, calls, events = worker_capture(monkeypatch, 2, 2)
    assert rc == 0, events[-1]
    tokens = [e for e in events if e['event'] == 'endpoint_created']
    assert len({e['endpoint_gid'] for e in tokens}) == 4
    assert len({e['endpoint_generation'] for e in tokens}) == 4
    assert calls.count('context') == calls.count('shutdown') == 1


from test_topology_participant_control_worker import transport, prepared_backend


def test_multi_backend_captures_every_active_group_at_snapshot_and_close(tmp_path, transport):
    p = multi_program(1, 2)
    backend = prepared_backend(tmp_path, p)
    handles = {e['logical_id']: backend.create_endpoint(e, 1, e['role']) for e in p['endpoints']}
    try:
        result = backend.snapshot(handles)
        assert result['complete'] and set(result['snapshots']) == {'w0', 'r0', 'r1'}
        captures = [e for e in backend.processes[0].events if e['event'] == 'participant_group_snapshot']
        assert len(captures) == 1
    finally:
        closed = backend.close()
    assert closed['closed'] and all(not h['alive'] for h in handles.values())
    captures = [e for e in backend.processes[0].events if e['event'] == 'participant_group_snapshot']
    assert len(captures) == 2


@pytest.mark.parametrize('writers,readers', [(1, 4), (4, 1), (4, 4)])
def test_admission_includes_both_bounded_extremes(writers, readers):
    p = multi_program(writers, readers)
    assert model.validate_program(p)['status'] == 'valid'


@pytest.mark.parametrize('damage', ['fifth_writer', 'fifth_reader', 'no_reader', 'duplicate_id',
                                    'wrong_role', 'last_topic', 'last_type', 'last_group',
                                    'repeat_create', 'missing_create', 'bool_generation', 'last_filter'])
def test_multi_input_fails_closed_before_resources(damage, tmp_path):
    p = multi_program(2, 2)
    if damage == 'fifth_writer': p = multi_program(5, 1)
    elif damage == 'fifth_reader': p = multi_program(1, 5)
    elif damage == 'no_reader': p = multi_program(2, 0)
    elif damage == 'duplicate_id': p['endpoints'][-1]['logical_id'] = 'w0'
    elif damage == 'wrong_role': p['endpoints'][-1]['role'] = 'writer_alias'
    elif damage == 'last_topic': p['endpoints'][-1]['topic'] = '/different'
    elif damage == 'last_type': p['endpoints'][-1]['type_name'] = 'std_msgs/msg/Int32'
    elif damage == 'last_group': p['endpoints'][-1]['participant_id'] = ''
    elif damage == 'repeat_create': p['actions'].append(action('again', 'create', 'r1', 1))
    elif damage == 'missing_create': p['actions'].pop()
    elif damage == 'bool_generation': p['actions'][-1]['generation'] = True
    elif damage == 'last_filter': p['endpoints'][-1]['filter'] = {}
    assert model.validate_program(p)['status'] == 'invalid'
    from r2f.runtime.topology_worker import RosTopologyBackend, TopologyUnsupported
    with pytest.raises(TopologyUnsupported, match='participant_control_scope'):
        RosTopologyBackend({}, tmp_path).prepare(p)


def test_missing_one_peer_capture_remains_unknown_despite_same_counts(tmp_path):
    f = multi_fixture(tmp_path, writers=2, readers=2)
    for e in f.events:
        if e['event'] == 'match_snapshot' and e['logical_id'] == 'r1':
            assert e['matched_count'] == 2
            e['peer_gids_complete'] = False
    resave(f)
    result = facts(f)
    assert result['diagnostic_valid'], result
    assert [(p['reader'], p['observed_match']) for p in result['state_after']['pairs']] == [
        ('r0', 'bidirectional'), ('r1', 'unknown'), ('r0', 'bidirectional'), ('r1', 'unknown')]


@pytest.mark.parametrize('damage', ['binding_missing', 'cross_group', 'cross_context', 'incarnation', 'role', 'alias_gid'])
def test_non_first_endpoint_forgery_has_no_report_authority(tmp_path, damage):
    f = multi_fixture(tmp_path, writers=2, readers=2, layout='roles')
    token = next(e for e in f.events if e['event'] == 'endpoint_created' and e['logical_id'] == 'r1')
    if damage == 'binding_missing':
        next(e for e in f.events if e['event'] == 'endpoint_group_bound'
             and e['endpoint']['logical_id'] == 'r1')['event'] = 'missing_binding'
    elif damage == 'cross_group': token['group_id'] = 'writer'
    elif damage == 'cross_context': token['context_instance_id'] = 'foreign'
    elif damage == 'incarnation': token['endpoint_generation'] = 'forged'
    elif damage == 'role': token['role'] = 'writer'
    elif damage == 'alias_gid':
        token['endpoint_gid'] = next(e['endpoint_gid'] for e in f.events
                                    if e['event'] == 'endpoint_created' and e['logical_id'] == 'r0')
    resave(f)
    result = facts(f)
    assert result.get('evidence_error'), result
    assert not result['candidate_eligible'] and not result['execution_evidence']


def test_multiwriter_labels_and_totals_do_not_establish_delivery_identity(tmp_path):
    f = multi_fixture(tmp_path, writers=2, readers=2, publisher_gid=False)
    for e in f.events:
        if e['event'] == 'receive':
            # Writer labels survive, but no strict body occurrence or native proof.
            e.pop('payload_sha256', None)
    resave(f)
    result = facts(f)
    assert result['diagnostic_valid'], result
    assert all(p['observed_match'] == 'bidirectional' for p in result['state_after']['pairs'])
    assert all(p['delivery_closure'] == 'unknown' for p in result['state_after']['pairs'])
    assert not result['candidate_eligible']


@pytest.mark.parametrize('writers,readers,edge_count', [(1, 4, 4), (4, 1, 4), (4, 4, 16)])
@pytest.mark.parametrize('layout', ['shared', 'roles', 'split'])
def test_bounded_extremes_project_each_identity_edge(tmp_path, writers, readers, edge_count, layout):
    result = facts(multi_fixture(tmp_path, writers=writers, readers=readers, layout=layout))
    assert result['diagnostic_valid'], result
    pairs = result['state_after']['pairs']
    assert len(pairs) == len({(p['writer'], p['reader']) for p in pairs}) == edge_count
    assert all(p['observed_match'] == 'bidirectional' for p in pairs)
    assert not result['candidate_eligible'] and not result['execution_evidence']


def test_4w4r_shared_carrier_binds_eight_distinct_endpoints(monkeypatch):
    rc, calls, events = worker_capture(monkeypatch, 4, 4)
    assert rc == 0, events[-1]
    tokens = [e for e in events if e['event'] == 'endpoint_created']
    assert len(tokens) == len({e['endpoint_gid'] for e in tokens}) == 8
    assert len({e['endpoint_generation'] for e in tokens}) == 8
    assert calls.count('context') == calls.count('shutdown') == 1
