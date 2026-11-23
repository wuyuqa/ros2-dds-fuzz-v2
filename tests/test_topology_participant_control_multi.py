"""CP2 bounded public input/carrier/artifact contracts, strictly offline fixtures."""
from copy import deepcopy

from r2f.modules.s1 import topology_model as model
from r2f.runtime.participant_groups import CONTROL
from test_s1_topology_model import action, endpoint, program


def caller_roles(p):
    from r2f.runtime.topology_worker import worker_role_for
    result = {}
    for kind in ('writer', 'reader'):
        for ordinal, ep in enumerate(e for e in p['endpoints'] if e['role'] == kind):
            result[ep['logical_id']] = worker_role_for(kind, ordinal)
    return result


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


def worker_capture(monkeypatch, writers=1, readers=2, *, layout='shared', group_id=None, damage=None, graph_view=None):
    """Use the established external ROS/OS fixture, with distinct observed GIDs."""
    import json
    import sys
    from r2f.runtime import topology_worker as worker
    from test_topology_participant_control_worker import ros_fixture, group_config
    p = multi_program(writers, readers, layout)
    cfg = group_config()
    group_id = group_id or p['endpoints'][0]['participant_id']
    cfg.update(group_id=group_id,
               group_members=[e for e in p['endpoints'] if e['participant_id'] == group_id], group_program=p,
               group_worker_roles=caller_roles(p))
    commands = [dict(command='create_endpoint', command_id=i, logical_id=e['logical_id'],
                     generation=1, worker_role=caller_roles(p)[e['logical_id']])
                for i, e in enumerate(cfg['group_members'], 1)]
    if damage is not None:
        damage(cfg, commands)
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
        if graph_view is not None:
            original_rows = Node.rows
            monkeypatch.setattr(Node, 'rows', lambda self, role: graph_view(
                role, original_rows(self, role), entities))
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


class CanonicalRawFixture(RawFixture):
    """Construct protocol observations with the runner's aliases at emission."""
    recorder_defaults = False
    def add(self, event, **fields):
        if fields.get('logical_id') is not None and 'role' in fields:
            alias = caller_roles(self.program)[fields['logical_id']]
            fields.update(role=alias, worker_role=alias)
        if self.recorder_defaults:
            gids = {'gid-' + e['logical_id']: f'{i:032x}'
                    for i, e in enumerate(self.program['endpoints'], 1)}
            def wire_gid(value):
                if isinstance(value, dict):
                    return {k: wire_gid(v) for k, v in value.items()}
                if isinstance(value, list):
                    return [wire_gid(v) for v in value]
                return gids.get(value, value) if isinstance(value, str) else value
            fields = wire_gid(fields)
            if event == 'worker_command_requested':
                event = 'topology_snapshot_requested'
            elif fields.get('logical_id') is not None:
                # Endpoint worker emit overrides the recorder default.
                alias = caller_roles(self.program)[fields['logical_id']]
                fields = {'role': alias, 'worker_role': alias, **fields}
            fields = {'role': 'topology_controller', **fields}
        return super().add(event, **fields)


from test_topology_participant_control_public import facts, resave


def multi_fixture(root, writers=1, readers=2, layout='shared', observed_ownership=False,
                  producer_framing=False, **kwargs):
    """Adapt the established raw protocol fixture, preserving all action evidence.

    Only supporting captures are unavailable. These rows are NOT measurements.
    """
    class RecorderFixture(CanonicalRawFixture):
        recorder_defaults = producer_framing
    f=RecorderFixture(writers=writers, readers=readers, **kwargs)
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
        t.update(rmw_identifier='rmw_cyclonedds_cpp',observed_domain_id=191,
                 domain_observation_source='offline_fixture',**identities[labels[eid]])
    additions=[]
    def row(kind,g,**kw):
        if producer_framing:
            # topology_pair.emit defaults actor role; child endpoint fields override it.
            # Group owner ACK/capture tokens contain worker_role, not role.
            eid = kw.get('logical_id')
            kw = {'role': 'topology_controller', **kw}
            if eid is not None and kind in {'command_done', 'endpoint_destroyed'}:
                kw['worker_role'] = caller_roles(f.program)[eid]
                if kind == 'endpoint_destroyed':
                    kw['role'] = kw['worker_role']
        return dict(event=kind,fixture=True,backend='offline_fake',real_ros=False,run_id='fixture-topology',
                    domain_id=191,observed_domain_id=191,rmw_identifier='rmw_cyclonedds_cpp',**identities[g],**kw)
    started=set()
    pending={}
    def request(g,command,eid=None,**kw):
        serial[g]+=1
        address={} if eid is None else {k:tokens[eid][k] for k in ('logical_id','generation','endpoint_generation')}
        if command=='create_endpoint':
            address.pop('endpoint_generation')
            if producer_framing: address['worker_role'] = caller_roles(f.program)[eid]
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
                if observed_ownership:
                    from r2f.runtime.topology_worker import _capture_group_ownership
                    sessions = {t['logical_id']: {'token': {**{k: t[k] for k in
                        ('logical_id', 'generation', 'endpoint_generation', 'endpoint_gid', 'worker_role')},
                        'kind': next(e['role'] for e in f.program['endpoints'] if e['logical_id'] == t['logical_id'])}}
                        for t in group_tokens}
                    obs.update(_capture_group_ownership({'group_id': label, 'domain_id': 191}, sessions))
                capture_token = ({k: group_tokens[0][k] for k in
                    ('logical_id', 'generation', 'endpoint_generation', 'endpoint_gid', 'worker_role')}
                    if producer_framing else {})
                additions.append(row('participant_group_snapshot',label,command_id=req['command_id'],**obs,**capture_token))
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


from test_topology_participant_control_worker import prepared_backend
@pytest.fixture
def transport(monkeypatch, tmp_path):
    """Labelled OS process/pipe/selector/lease doubles, never real spawn/locks."""
    from copy import deepcopy
    from itertools import count
    from r2f.runtime import topology_worker as worker
    allocated=[]; pids=count(3000)
    class Pipe:
        def __init__(self,child,read=False):self.child=child;self.reading=read
        def fileno(self):return 88
        def read(self):data=self.child.output;self.child.output=b'';return data
        def write(self,data):self.child.handle(json.loads(data));return len(data)
        def flush(self):pass
        def close(self):pass
    class Child:
        def __init__(self,argv,**kw):
            self.cfg=json.loads(argv[-2]);self.pid=next(pids);self.returncode=None;self.output=b'';self.tokens={}
            self.stdin=Pipe(self);self.stdout=Pipe(self,True);self.bad_ack=False
            allocated.append(self)
            for event in ('group_context_created','group_worker_ready'):
                self.add(event,api='rclpy.context.Context+rclpy.init',ros_context_created=True)
        def add(self,event,**fields):
            value={'event':event,'pid':self.pid,'pgid':self.pid,'process_start_ticks':900,
                'domain_id':173,'observed_domain_id':173,'rmw_identifier':'rmw_cyclonedds_cpp',
                'run_id':'offline-fixture','domain_observation_source':'fixture_api','monotonic_ns':__import__('time').monotonic_ns(),
                **{k:self.cfg[k] for k in ('group_id','worker_instance_id','context_instance_id')},**fields}
            self.output+=json.dumps(value).encode()+b'\n'
        def handle(self,c):
            op=c['command'];cid=c['command_id'];eid=c.get('logical_id')
            if op=='stop':
                for token in self.tokens.values():self.add('endpoint_destroyed',**token,cleanup_status='closed')
                self.add('command_done',command='stop',command_id=cid)
                self.add('group_context_shutdown',closed=True,errors=[])
                self.returncode=0;return
            if op=='create_endpoint':
                spec=next(e for e in self.cfg['group_members'] if e['logical_id']==eid)
                token=dict(logical_id=eid,generation=c['generation'],endpoint_generation=f"fixture-{eid}-g{c['generation']}",
                    endpoint_gid=f"{list(self.cfg['group_worker_roles']).index(eid)+1+(c['generation']-1)*8:032x}",
                    role=c['worker_role'],worker_role=c['worker_role'])
                self.tokens[eid]=token
                from r2f.runtime.topology_worker import endpoint_expressibility
                q=endpoint_expressibility(spec,{})['public_qos']
                self.add('endpoint_created',**token,topic=spec['topic'],message_type=spec['type_name'],qos=q,
                    node_name='fixture-group-node',created_monotonic_ns=100,
                    identity_source='get_publishers_info_by_topic' if spec['role']=='writer' else 'get_subscriptions_info_by_topic')
                self.add('endpoint_group_bound',endpoint={k:token[k] for k in ('logical_id','generation','endpoint_generation','endpoint_gid','worker_role')})
            if op == 'group_snapshot' and eid is None:
                assert not self.tokens
                self.add('participant_group_snapshot', command_id=cid,
                    schema='r2f.ros-participant-group-observation/v1', supporting_diagnostic=True, candidate_eligible=False,
                    requested_grouping=self.cfg['group_id'], ros_context_application_observed=True,
                    endpoint_ownership_observed=False, local_root_inventory_complete=False, group_relation_observed=False,
                    actual_local_root_count=None, roots_before=None, roots_after=None, endpoints=[],
                    unknown_reasons=['local_root_inventory_incomplete'], endpoint_absence_observed=True, active_endpoint_count=0)
                self.add('command_done', command=op, command_id=cid, endpoint_absence_observed=True, active_endpoint_count=0)
                return
            assert eid in self.tokens, ('retired_endpoint_command', op, eid)
            token=deepcopy(self.tokens[eid])
            if op=='destroy_endpoint':
                self.add('endpoint_destroyed',**token,cleanup_status='closed')
                self.add('command_done',**token,command=op,command_id=cid,cleanup_status='closed')
                del self.tokens[eid]
                return
            if op=='snapshot':
                self.add('match_snapshot',**token,command_id=cid,matched_count=1)
                pubs=[dict(endpoint_gid=t['endpoint_gid'],node_name='fixture-group-node',topic_type='std_msgs/msg/String',qos={}) for t in self.tokens.values() if t['role'].startswith('writer')]
                subs=[dict(endpoint_gid=t['endpoint_gid'],node_name='fixture-group-node',topic_type='std_msgs/msg/String',qos={}) for t in self.tokens.values() if t['role'].startswith('reader')]
                self.add('graph_snapshot',**token,command_id=cid,publishers=pubs,subscriptions=subs)
            elif op=='publish_batch':
                for sequence in c['sequences']:
                    body = dict(run_id=self.cfg['run_id'], writer_id=eid, writer_generation=token['generation'],
                                endpoint_gid=token['endpoint_gid'], send_sequence=sequence, phase=c['phase'])
                    data = json.dumps(body, sort_keys=True)
                    digest = hashlib.sha256(data.encode()).hexdigest()
                    self.add('send', **token, command_id=cid, send_sequence=sequence, writer_gid=token['endpoint_gid'],
                             phase=c['phase'], string_data=data, payload_sha256=digest, publish_returned=True)
                    for child in allocated:
                        for reader in child.tokens.values():
                            if reader['worker_role'].startswith('reader'):
                                child.add('receive', **reader, send_sequence=sequence, phase=c['phase'],
                                    publisher_gid=None, publisher_gid_status='unavailable_api', identity_basis='endpoint_correlated',
                                    payload_run_id=body['run_id'], payload_writer_id=eid,
                                    payload_writer_generation=token['generation'], payload_endpoint_gid=token['endpoint_gid'],
                                    string_data=data, payload_sha256=digest)
            elif op=='group_snapshot':
                self.add('participant_group_snapshot',**token,command_id=cid,
                    schema='r2f.ros-participant-group-observation/v1',supporting_diagnostic=True,candidate_eligible=False,
                    requested_grouping=self.cfg['group_id'],ros_context_application_observed=True,
                    endpoint_ownership_observed=False,local_root_inventory_complete=False,group_relation_observed=False,
                    actual_local_root_count=None,roots_before=None,roots_after=None,endpoints=[],
                    unknown_reasons=['local_root_inventory_incomplete'])
            if self.bad_ack:token['logical_id']='cross-address'
            self.add('command_done',**token,command_id=cid,command=op)
        def poll(self):return self.returncode
        def wait(self,**kw):return self.returncode
    class Selector:
        def register(self,*a):pass
        def unregister(self,*a):pass
        def close(self):pass
        def select(self,*a):return [(None,None)]
    monkeypatch.setattr(worker.subprocess,'Popen',Child)
    monkeypatch.setattr(worker.selectors,'DefaultSelector',Selector)
    monkeypatch.setattr(worker.os,'set_blocking',lambda *a:None)
    original=worker.Path.read_text
    monkeypatch.setattr(worker.Path,'read_text',lambda p,*a,**kw: '100 (fixture) '+' '.join(['0']*19+['900']) if str(p).startswith('/proc/') else original(p,*a,**kw))
    import r2f.runtime.ros2_pair as pair
    monkeypatch.setattr(pair,'verify_pair_lease',lambda *a:[{'fixture':True}]*3)
    return allocated




def test_multi_backend_captures_every_active_group_at_snapshot_and_close(tmp_path, transport):
    p = multi_program(1, 2)
    backend = prepared_backend(tmp_path, p)
    handles = {e['logical_id']: backend.create_endpoint(e, 1, caller_roles(p)[e['logical_id']]) for e in p['endpoints']}
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


@pytest.mark.parametrize('writers,readers', [(1, 2), (2, 2), (4, 4)])
def test_canonical_caller_worker_aliases(monkeypatch, writers, readers):
    rc, _, events = worker_capture(monkeypatch, writers, readers)
    assert rc == 0, events[-1]
    tokens = [e for e in events if e['event'] == 'endpoint_created']
    expected = caller_roles(multi_program(writers, readers))
    assert {e['logical_id']: e['role'] for e in tokens} == expected
    assert {e['logical_id']: e['worker_role'] for e in tokens} == expected


@pytest.mark.parametrize('layout,group_id,expected', [
    ('split', 'r1', {'r1': 'reader2'}),
    ('split', 'w1', {'w1': 'writer2'}),
    ('roles', 'reader', {'r0': 'reader', 'r1': 'reader2'})])
def test_worker_global_aliases_survive_group_subset(monkeypatch, layout, group_id, expected):
    rc, _, events = worker_capture(monkeypatch, 2, 2, layout=layout, group_id=group_id)
    assert rc == 0, events[-1]
    assert {e['logical_id']: e['worker_role'] for e in events if e['event'] == 'endpoint_created'} == expected


@pytest.mark.parametrize('damage', ['wrong_kind', 'duplicate_alias', 'swapped_alias', 'swapped_logical_id', 'wrong_logical_id', 'duplicate_create'])
def test_worker_rejects_noncanonical_endpoint_address(monkeypatch, damage):
    def corrupt(cfg, commands):
        if damage == 'wrong_kind': commands[-1]['worker_role'] = 'writer2'
        elif damage == 'duplicate_alias': commands[-1]['worker_role'] = 'reader'
        elif damage == 'swapped_alias':
            commands[-2]['worker_role'], commands[-1]['worker_role'] = 'reader2', 'reader'
        elif damage == 'swapped_logical_id':
            commands[-2]['logical_id'], commands[-1]['logical_id'] = 'r1', 'r0'
        elif damage == 'wrong_logical_id': commands[-1]['logical_id'] = 'foreign'
        else: commands[-1]['logical_id'], commands[-1]['worker_role'] = 'r0', 'reader'
    rc, _, events = worker_capture(monkeypatch, damage=corrupt)
    assert rc == 1
    assert events[-1]['event'] == 'worker_error'
    assert not any(e['event'] == 'endpoint_created' and e['logical_id'] == 'r1' for e in events)


@pytest.mark.parametrize('layout', ['shared', 'roles', 'split'])
@pytest.mark.parametrize('writers,readers', [(1, 2), (2, 2), (4, 4)])
def test_backend_transmits_global_exact_alias_binding(tmp_path, transport, layout, writers, readers):
    p = multi_program(writers, readers, layout)
    backend = prepared_backend(tmp_path, p)
    try:
        for e in p['endpoints']:
            h = backend.create_endpoint(e, 1, caller_roles(p)[e['logical_id']])
            assert h['worker_role'] == caller_roles(p)[e['logical_id']]
        for child in transport:
            assert child.cfg['group_program'] == p
            assert child.cfg['group_worker_roles'] == caller_roles(p)
        assert backend.snapshot(backend.handles)['complete']
    finally:
        assert backend.close()['closed']


@pytest.mark.parametrize('alias', ['writer2', 'reader', 'reader3'])
def test_backend_rejects_wrong_caller_alias_before_spawning(tmp_path, transport, alias):
    from r2f.runtime.topology_worker import TopologyBackendError
    p = multi_program(layout='split')
    backend = prepared_backend(tmp_path, p)
    try:
        for e in p['endpoints'][:2]:
            backend.create_endpoint(e, 1, caller_roles(p)[e['logical_id']])
        count = len(transport)
        with pytest.raises(TopologyBackendError, match='group_endpoint_worker_role_mismatch'):
            backend.create_endpoint(p['endpoints'][2], 1, alias)
        assert len(transport) == count
    finally:
        assert backend.close()['closed']


@pytest.mark.parametrize('field', ['role', 'worker_role'])
def test_lineage_rejects_consistent_same_kind_alias_swap(tmp_path, field):
    f = multi_fixture(tmp_path, 2, 2)
    # Even a consistently forged stream cannot swap declaration-order identity.
    for e in f.events:
        if e.get('logical_id') in ('r0', 'r1') and field in e:
            e[field] = 'reader2' if e['logical_id'] == 'r0' else 'reader'
    with pytest.raises(ValueError, match='endpoint_identity|role_alias'):
        project_lineage(f.program, f.events, str(f.raw), config=f.config)


def test_numbered_role_double_sample_producer_projects_same_observed_status(monkeypatch, tmp_path):
    from r2f.runtime import topology_worker as worker
    class NativeFixture:
        """External ownership ABI double only; no native library or ROS."""
        def sample_roots(self, domain):
            return {'complete': True, 'domain_id': domain, 'count': 1,
                    'provenance': {'fixture': True, 'not_native': True},
                    'roots': [{'participant_handle': 10, 'participant_gid': f'{90:032x}',
                               'domain_id': domain, 'domain_return_code': 0, 'parent': 0,
                               'participant_self_return': 10, 'valid': True}]}
        def sample_ownership(self, domain, gid, kind):
            h = (20 if kind == 'writer' else 30) + int(gid.rsplit('-', 1)[-1][1:])
            return {'schema': 'r2f.cyclone-participant-ownership/v2', 'complete': True,
                    'status': 'observed', 'candidate_eligible': False, 'stable_sample_count': 2,
                    'domain_id': domain, 'local_gid': gid, 'role_kind': kind, 'local_handle': h,
                    'participant_handle': 10, 'participant_gid': f'{90:032x}', 'parent_chain': [h, 10],
                    'entity_inventory': [[10, [h]], [h, []]],
                    'entity_identities': [{'handle': h, 'gid': gid}, {'handle': 10, 'gid': f'{90:032x}'}],
                    'participant_root_observation': {'participant_handle': 10, 'participant_self_return': 10,
                                                     'lookup_participants': [10]},
                    'provenance': {'fixture': True, 'not_native': True}}
    monkeypatch.setattr(worker, 'CycloneMatchedPeerObserver', NativeFixture)
    f = multi_fixture(tmp_path, 2, 2, observed_ownership=True)
    captures = [e for e in f.events if e['event'] == 'participant_group_snapshot']
    assert all(e['endpoint_ownership_observed'] and e['group_relation_observed'] for e in captures)
    assert {e['worker_role'] for e in captures[0]['endpoints']} == {'writer', 'writer2', 'reader', 'reader2'}
    lineage = project_lineage(f.program, f.events, str(f.raw), config=f.config)
    assert all(o['endpoint_ownership_observed'] and o['group_relation_observed'] for o in lineage['observations'])
    assert facts(f)['diagnostic_valid']


@pytest.mark.parametrize('field', ['role', 'worker_role'])
def test_backend_readback_rejects_same_kind_alias_impersonation(tmp_path, transport, field):
    from r2f.runtime.topology_worker import TopologyBackendError
    p = multi_program()
    backend = prepared_backend(tmp_path, p)
    try:
        for e in p['endpoints'][:2]:
            backend.create_endpoint(e, 1, caller_roles(p)[e['logical_id']])
        child = transport[0]
        original = child.add
        def add(event, **fields):
            if event == 'endpoint_created' and fields.get('logical_id') == 'r1':
                fields[field] = 'reader'  # Explicit negative OS-boundary fixture.
            return original(event, **fields)
        child.add = add
        with pytest.raises(TopologyBackendError):
            backend.create_endpoint(p['endpoints'][2], 1, 'reader2')
        assert not any(e['event'] == 'endpoint_created' and e.get('logical_id') == 'r1'
                       for e in backend.processes[0].events)
    finally:
        backend.close()


@pytest.mark.parametrize('damage', ['missing_map', 'swapped_map', 'subset_renumber'])
def test_worker_rejects_unbound_global_alias_map(monkeypatch, damage):
    def corrupt(cfg, commands):
        if damage == 'missing_map': cfg.pop('group_worker_roles')
        elif damage == 'swapped_map':
            cfg['group_worker_roles']['r0'], cfg['group_worker_roles']['r1'] = 'reader2', 'reader'
        else: cfg['group_worker_roles']['r1'] = 'reader'
    rc, calls, events = worker_capture(monkeypatch, 2, 2, layout='split', group_id='r1', damage=corrupt)
    assert rc == 1 and events[-1]['event'] == 'worker_error'
    assert 'context' not in calls


@pytest.mark.parametrize('writers,readers', [(1, 2), (2, 2)])
@pytest.mark.parametrize('layout', ['shared', 'roles'])
def test_recorder_actor_framing_reaches_lineage_and_pair_projection(tmp_path, writers, readers, layout):
    f = multi_fixture(tmp_path, writers, readers, layout=layout, producer_framing=True)
    controller = [e for e in f.events if e.get('logical_id') and
                  e.get('role') == 'topology_controller']
    assert {e['event'] for e in controller} >= {
        'group_command_requested', 'topology_snapshot_requested',
        'participant_group_snapshot', 'command_done'}
    observed = facts(f)
    assert observed['diagnostic_valid'], observed
    assert all(p['observed_match'] == 'bidirectional' for p in observed['state_after']['pairs'])
    assert not observed['candidate_eligible'] and not observed['execution_evidence']


def recorder_pair_facts(f):
    """Observe the public pair consumer at each final worker snapshot prefix."""
    from r2f.runtime.topology_pair import pair_graph_facts
    tokens = {e['logical_id']: e for e in f.events if e['event'] == 'endpoint_created'}
    snapshots = {}
    limits = []
    for eid in tokens:
        match = next(e for e in reversed(f.events)
                     if e['event'] == 'match_snapshot' and e.get('logical_id') == eid)
        cid = match['command_id']
        graph = next(e for e in f.events if e['event'] == 'graph_snapshot'
                     and e.get('logical_id') == eid and e.get('command_id') == cid)
        ack = next(e for e in f.events if e['event'] == 'command_done'
                   and e.get('logical_id') == eid and e.get('command_id') == cid)
        limits.append(ack['event_index'] + 1)
        snapshots[eid] = dict(command_id=cid, complete=True,
                              match_snapshot=match, graph_snapshot=graph)
    return [pair_graph_facts(tokens[w['logical_id']], tokens[r['logical_id']],
                snapshots[w['logical_id']], snapshots[r['logical_id']],
                raw_events=f.events, before_index=max(limits), real_ros=False)
            for w in f.program['endpoints'] if w['role'] == 'writer'
            for r in f.program['endpoints'] if r['role'] == 'reader']


@pytest.mark.parametrize('writers,readers', [(1, 2), (2, 2)])
@pytest.mark.parametrize('layout', ['shared', 'roles'])
def test_recorder_actor_framing_authenticates_live_pair_scope(tmp_path, writers, readers, layout):
    f = multi_fixture(tmp_path, writers, readers, layout=layout, producer_framing=True)
    pairs = recorder_pair_facts(f)
    assert len(pairs) == writers * readers
    assert all(p['status'] == 'matched' for p in pairs), [(p['writer'], p['reader'], p['reasons']) for p in pairs]
    assert all(not p['execution_evidence'] and not p['candidate_eligible'] for p in pairs)


@pytest.mark.parametrize('event,command,field,value', [
    ('group_command_requested', 'create_endpoint', 'role', 'reader2'),
    ('group_command_requested', 'create_endpoint', 'worker_role', 'reader2'),
    ('group_command_requested', 'create_endpoint', 'role', 'forged_controller'),
    ('command_done', 'create_endpoint', 'role', 'reader2'),
    ('command_done', 'create_endpoint', 'worker_role', 'reader2'),
    ('topology_snapshot_requested', 'snapshot', 'role', 'reader2'),
    ('topology_snapshot_requested', 'snapshot', 'worker_role', 'reader2'),
    ('match_snapshot', None, 'role', 'topology_controller'),
    ('graph_snapshot', None, 'role', 'topology_controller'),
    ('endpoint_created', None, 'role', 'topology_controller'),
    ('command_done', None, 'role', 'topology_controller'),
])
def test_recorder_actor_exception_rejects_forged_endpoint_and_actor_roles(
        tmp_path, event, command, field, value):
    f = multi_fixture(tmp_path, producer_framing=True)
    target = next(e for e in f.events if e['event'] == event
                  and e.get('logical_id') == 'r0' and e.get('command') == command)
    target[field] = value  # Explicit negative capture, not recorder framing.
    with pytest.raises(ValueError, match='role_alias|endpoint_identity'):
        project_lineage(f.program, f.events, str(f.raw), config=f.config)
    assert all(p['status'] == 'unknown' for p in recorder_pair_facts(f))


@pytest.mark.parametrize('field,value', [('role', 'reader2'), ('worker_role', 'reader2'),
                                        ('role', 'forged_controller')])
def test_group_capture_actor_does_not_excuse_forged_endpoint_alias(tmp_path, field, value):
    f = multi_fixture(tmp_path, producer_framing=True)
    capture = next(e for e in f.events if e['event'] == 'participant_group_snapshot')
    capture[field] = value
    with pytest.raises(ValueError, match='endpoint_event_role_alias'):
        project_lineage(f.program, f.events, str(f.raw), config=f.config)


def cp3_worker_capture(monkeypatch, *, bad_destroy_nonce=False, fail_destroy=False, scenario_commands=None, graph_view=None):
    """External ROS/pipe fixture exercises the existing worker entry, no DDS."""
    import sys
    from r2f.runtime import topology_worker as worker
    from test_topology_participant_control_worker import ros_fixture, group_config, create
    commands = [create('w0', 1), create('r0', 2),
                dict(command='destroy_endpoint', command_id=3, logical_id='r0', generation=1),
                dict(command='create_endpoint', command_id=4, logical_id='r0', generation=2, worker_role='reader'),
                dict(command='snapshot', command_id=5, logical_id='w0', generation=1),
                dict(command='snapshot', command_id=6, logical_id='r0', generation=2),
                dict(command='group_snapshot', command_id=7, logical_id='w0', generation=1),
                dict(command='stop', command_id=8)]
    if scenario_commands is not None:
        commands = scenario_commands
    checkpoints = []
    with ros_fixture(monkeypatch, [], fail_destroy=fail_destroy) as (calls, entities, stream):
        Node = sys.modules['rclpy.node'].Node
        serial = [0]
        if fail_destroy:
            destroy = Node.destroy_subscription
            def failed_destroy(self, endpoint):
                destroy(self, endpoint)
                return False
            monkeypatch.setattr(Node, 'destroy_subscription', failed_destroy)
        for method in ('create_publisher', 'create_subscription'):
            original = getattr(Node, method)
            def create_entity(self, *args, _create=original, **kwargs):
                e = _create(self, *args, **kwargs)
                serial[0] += 1
                e.gid = f'{serial[0]:032x}'
                return e
            monkeypatch.setattr(Node, method, create_entity)
        class PeerAPI:
            # External ABI fixture observes only the currently allocated endpoints.
            def sample(self, domain, gid, kind):
                return dict(complete=True, domain_id=domain, local_gid=gid, role_kind=kind,
                            matched_peer_gids=[e.gid for e in entities if e.role != kind])
        monkeypatch.setattr(worker, 'CycloneMatchedPeerObserver', PeerAPI)
        pending = iter(commands)
        def read_command(*_):
            command = next(pending, None)
            if command is None:
                return b''
            rows = [json.loads(s) for s in stream.getvalue().splitlines()]
            if command['command'] in {'destroy_endpoint', 'snapshot', 'group_snapshot', 'publish_batch'} and command.get('logical_id') is not None:
                token = next(e for e in reversed(rows) if e['event'] == 'endpoint_created'
                             and e['logical_id'] == command['logical_id'])
                command['endpoint_generation'] = ('wrong' if bad_destroy_nonce and command['command'] == 'destroy_endpoint'
                                                  else token['endpoint_generation'])
            identity = {k: rows[0][k] for k in ('pid', 'pgid', 'process_start_ticks', 'group_id', 'worker_instance_id', 'context_instance_id', 'run_id', 'domain_id')}
            request = dict(event='group_command_requested', role='topology_controller', **identity, **command)
            stream.write(json.dumps(request) + '\n')
            if command['command'] == 'snapshot':
                stream.write(json.dumps({**request, 'event': 'topology_snapshot_requested', 'endpoint_gid': token['endpoint_gid']}) + '\n')
            checkpoints.append((command['command_id'], len(entities), calls.count('shutdown')))
            return json.dumps(command).encode() + b'\n'
        if graph_view is not None:
            original_rows = Node.rows
            monkeypatch.setattr(Node, 'rows', lambda self, role: graph_view(
                role, original_rows(self, role), entities))
        monkeypatch.setattr(worker.os, 'read', read_command)
        monkeypatch.setattr(sys, 'argv', ['worker', json.dumps(group_config()), 'writer'])
        rc = worker.worker_main()
    return rc, calls, [json.loads(s) for s in stream.getvalue().splitlines()], checkpoints


def test_group_endpoint_destroy_recreate_keeps_sibling_and_context(monkeypatch):
    rc, calls, rows, checkpoints = cp3_worker_capture(monkeypatch)
    assert rc == 0, rows[-1]
    assert (4, 1, 0) in checkpoints  # reader gone; writer and context survive
    assert (5, 2, 0) in checkpoints
    assert calls.count('context') == calls.count('shutdown') == 1
    created = [e for e in rows if e['event'] == 'endpoint_created']
    assert [(e['logical_id'], e['generation']) for e in created] == [('w0', 1), ('r0', 1), ('r0', 2)]
    assert len({e['endpoint_gid'] for e in created}) == len({e['endpoint_generation'] for e in created}) == 3
    retired = next(e for e in rows if e['event'] == 'endpoint_destroyed' and e['logical_id'] == 'r0')
    ack = next(e for e in rows if e['event'] == 'command_done' and e['command_id'] == 3)
    assert rows.index(created[1]) < rows.index(retired) < rows.index(ack) < rows.index(created[2])
    assert retired['cleanup_status'] == ack['cleanup_status'] == 'closed'
    assert ack['endpoint_generation'] == created[1]['endpoint_generation']
    assert any(e['event'] == 'command_done' and e.get('command_id') == 5 and e.get('logical_id') == 'w0' for e in rows)


def cp3_lineage_fixture(monkeypatch):
    from test_topology_participant_control_worker import group_config
    cfg = group_config()
    p = program(deepcopy(cfg['group_members']))
    p['participant_control'] = dict(CONTROL)
    p['actions'] += [action('destroy_r0', 'destroy', 'r0', 1), action('recreate_r0', 'recreate', 'r0', 2)]
    rc, _, rows, _ = cp3_worker_capture(monkeypatch)
    assert rc == 0, rows[-1]
    identity = {k: rows[0][k] for k in ('pid', 'pgid', 'process_start_ticks', 'group_id', 'worker_instance_id', 'context_instance_id', 'run_id', 'domain_id')}
    events = [dict(event='input_binding', run_id=cfg['run_id'], receipt=dict(program_sha256=canonical_hash(p), participant_control=dict(CONTROL))),
              dict(event='process_started', fresh_process=True, separate_context=False, **identity), *rows,
              dict(event='process_exit', returncode=0, **identity)]
    for i, e in enumerate(events):
        e.update(event_index=i, monotonic_ns=1000+i*1000, fixture=True, real_ros=False, backend='offline_fake')
    return p, events


def test_recreated_group_lineage_projects_all_incarnations(monkeypatch, tmp_path):
    p, events = cp3_lineage_fixture(monkeypatch)
    (tmp_path/'fixture.jsonl').write_text(''.join(json.dumps(e, sort_keys=True)+'\n' for e in events))
    lineage = project_lineage(p, events, tmp_path/'fixture.jsonl')
    assert [(w['logical_id'], w['generation']) for w in lineage['workers']] == [('w0', 1), ('r0', 1), ('r0', 2)]
    assert len(lineage['processes']) == len(lineage['contexts']) == 1
    assert all(w['closed'] and not w['real_ros'] for w in lineage['workers'])
    assert not lineage['candidate_eligible'] and not lineage['group_relation_observed']


def test_recreated_pair_consumer_excludes_old_relation_in_shared_process(monkeypatch):
    from r2f.runtime.topology_pair import pair_graph_facts
    p, rows = cp3_lineage_fixture(monkeypatch)
    tokens = {e['logical_id']: e for e in rows if e['event'] == 'endpoint_created'}
    snapshots = {}
    for eid in tokens:
        match = next(e for e in rows if e['event'] == 'match_snapshot' and e['logical_id'] == eid)
        cid = match['command_id']
        graph = next(e for e in rows if e['event'] == 'graph_snapshot' and e.get('logical_id') == eid and e.get('command_id') == cid)
        snapshots[eid] = dict(command_id=cid, complete=True, match_snapshot=match, graph_snapshot=graph)
    limit = next(e['event_index'] for e in rows if e['event'] == 'command_done' and e['command_id'] == 6) + 1
    pair = pair_graph_facts(tokens['w0'], tokens['r0'], snapshots['w0'], snapshots['r0'],
                            raw_events=rows, before_index=limit, real_ros=False)
    assert pair['status'] == 'matched', pair['reasons']
    assert pair['reader_generation'] == 2
    old = next(e for e in rows if e['event'] == 'endpoint_created' and e['logical_id'] == 'r0')
    stale = pair_graph_facts(tokens['w0'], old, snapshots['w0'], snapshots['r0'],
                            raw_events=rows, before_index=limit, real_ros=False)
    assert stale['status'] == 'unknown'


def test_recreated_group_final_audit_preserves_earlier_retirement(monkeypatch, tmp_path):
    from r2f.runtime.topology_pair import group_termination_rows
    p, events = cp3_lineage_fixture(monkeypatch)
    tokens = {e['logical_id']: e for e in events if e['event'] == 'endpoint_created'}
    boundary = next(e for e in events if e['event'] == 'command_done' and e['command_id'] == 6)
    rows = group_termination_rows(p, events, tmp_path/'fixture.jsonl', list(tokens.values()), boundary)
    assert all(e['event_index'] > boundary['event_index'] for e in rows)
    assert {(e['logical_id'], e['generation']) for e in rows if e['event'] == 'endpoint_destroyed'} == {('w0', 1), ('r0', 2)}
    # A retired generation cannot be presented as active at the final boundary.
    old = next(e for e in events if e['event'] == 'endpoint_created' and e['logical_id'] == 'r0')
    with pytest.raises(ValueError):
        group_termination_rows(p, events, tmp_path/'fixture.jsonl', [old], boundary)


def test_numbered_endpoint_retirement_preserves_exact_alias_and_siblings(tmp_path, transport):
    p = multi_program(2, 2)
    p['actions'] += [action('destroy_r1', 'destroy', 'r1', 1), action('recreate_r1', 'recreate', 'r1', 2)]
    backend = prepared_backend(tmp_path, p)
    rows = []
    backend.set_event_sink(lambda event, **fields: rows.append(dict(event=event, **fields)))
    handles = {e['logical_id']: backend.create_endpoint(e, 1, caller_roles(p)[e['logical_id']]) for e in p['endpoints']}
    old = handles['r1']
    assert backend.destroy_endpoint(old)['process_retained']
    retired = next(e for e in rows if e['event'] == 'topology_endpoint_destroyed')
    assert retired['role'] == 'reader2'
    assert all(h['alive'] for eid, h in handles.items() if eid != 'r1')
    new = backend.create_endpoint(p['endpoints'][-1], 2, 'reader2')
    assert new['generation'] == 2 and new['pid'] == old['pid']
    assert new['endpoint_gid'] != old['endpoint_gid']
    assert len(transport) == 1
    assert backend.close()['closed']


@pytest.mark.parametrize('damage', ['missing_destroy_ack', 'wrong_destroy_nonce', 'failed_cleanup',
    'duplicate_cleanup', 'destroy_ack_before_cleanup', 'reused_gid', 'reused_nonce',
    'wrong_generation', 'changed_context', 'wrong_alias', 'late_old_activity', 'backdated_destroy', 'backdated_create_ack'])
def test_recreated_group_lineage_rejects_forged_or_incomplete_history(monkeypatch, tmp_path, damage):
    p, rows = cp3_lineage_fixture(monkeypatch)
    tokens = [e for e in rows if e['event'] == 'endpoint_created']
    old, new = tokens[1:]
    cleanup = next(e for e in rows if e['event'] == 'endpoint_destroyed' and e['logical_id'] == 'r0')
    ack = next(e for e in rows if e['event'] == 'command_done' and e['command_id'] == 3)
    req = next(e for e in rows if e['event'] == 'group_command_requested' and e['command_id'] == 3)
    if damage == 'missing_destroy_ack': ack['event'] = 'missing_ack'
    elif damage == 'wrong_destroy_nonce': req['endpoint_generation'] = 'foreign'
    elif damage == 'failed_cleanup': cleanup['cleanup_status'] = 'unknown'
    elif damage == 'duplicate_cleanup': rows.insert(rows.index(cleanup), deepcopy(cleanup))
    elif damage == 'destroy_ack_before_cleanup':
        rows.remove(ack); rows.insert(rows.index(cleanup), ack)
    elif damage == 'reused_gid': new['endpoint_gid'] = old['endpoint_gid']
    elif damage == 'reused_nonce': new['endpoint_generation'] = old['endpoint_generation']
    elif damage == 'wrong_generation': new['generation'] = 3
    elif damage == 'changed_context': new['context_instance_id'] = 'foreign'
    elif damage == 'wrong_alias': ack['worker_role'] = 'reader2'
    elif damage == 'late_old_activity':
        activity = next(e for e in rows if e['event'] == 'graph_snapshot' and e.get('logical_id') == 'r0')
        rows.insert(rows.index(new)+1, deepcopy(activity))
    elif damage == 'backdated_destroy': req['monotonic_ns'] = ack['monotonic_ns'] + 1
    elif damage == 'backdated_create_ack':
        next(e for e in rows if e['event'] == 'command_done' and e['command_id'] == 4)['monotonic_ns'] = new['monotonic_ns'] - 1
    for i, e in enumerate(rows): e['event_index'] = i
    with pytest.raises(ValueError, match='group_lineage:'):
        project_lineage(p, rows, tmp_path/'negative-fixture.jsonl')


def test_worker_rejects_stale_destroy_nonce_without_success_ack(monkeypatch):
    rc, _, rows, _ = cp3_worker_capture(monkeypatch, bad_destroy_nonce=True)
    assert rc == 1 and rows[-1]['event'] == 'worker_error'
    assert not any(e['event'] == 'command_done' and e.get('command_id') == 3 for e in rows)
    assert not any(e['event'] == 'endpoint_created' and e['generation'] == 2 for e in rows)


def test_worker_failed_endpoint_destroy_keeps_unknown_raw_without_success_ack(monkeypatch):
    rc, _, rows, _ = cp3_worker_capture(monkeypatch, fail_destroy=True)
    assert rc == 1 and rows[-1]['event'] == 'worker_error'
    cleanup = next(e for e in rows if e['event'] == 'endpoint_destroyed' and e['logical_id'] == 'r0')
    assert cleanup['cleanup_status'] == 'unknown'
    assert not any(e['event'] == 'command_done' and e.get('command_id') == 3 for e in rows)
    assert not any(e['event'] == 'endpoint_created' and e['generation'] == 2 for e in rows)


def test_worker_writer_reincarnation_and_bounded_empty_group_deliver_new_generation(monkeypatch, tmp_path):
    # One command stream through the worker entry; doubles only at ROS/OS boundaries.
    commands = [
        dict(command='create_endpoint', logical_id='w0', generation=1, worker_role='writer'),
        dict(command='create_endpoint', logical_id='r0', generation=1, worker_role='reader'),
        dict(command='destroy_endpoint', logical_id='w0', generation=1),
        dict(command='snapshot', logical_id='r0', generation=1),
        dict(command='create_endpoint', logical_id='w0', generation=2, worker_role='writer'),
        dict(command='publish_batch', logical_id='w0', generation=2, sequences=[0], interval_sec=0, phase='writer_reborn'),
        dict(command='destroy_endpoint', logical_id='w0', generation=2),
        dict(command='destroy_endpoint', logical_id='r0', generation=1),
        dict(command='group_snapshot'),
        dict(command='group_snapshot'),
        dict(command='create_endpoint', logical_id='w0', generation=3, worker_role='writer'),
        dict(command='create_endpoint', logical_id='r0', generation=2, worker_role='reader'),
        dict(command='publish_batch', logical_id='w0', generation=3, sequences=[0], interval_sec=0, phase='all_reborn'),
        dict(command='snapshot', logical_id='r0', generation=2),
        dict(command='stop'),
    ]
    for cid, command in enumerate(commands, 1):
        command['command_id'] = cid
    rc, calls, rows, checkpoints = cp3_worker_capture(monkeypatch, scenario_commands=commands)
    (tmp_path/'offline-fixture-worker.jsonl').write_text(''.join(json.dumps(dict(e, fixture=True, real_ros=False))+'\n' for e in rows))
    assert rc == 0, rows[-1]
    assert (4, 1, 0) in checkpoints  # reader remains while writer is retired
    assert (9, 0, 0) in checkpoints and (11, 0, 0) in checkpoints
    assert calls.count('context') == calls.count('shutdown') == 1
    created = [e for e in rows if e['event'] == 'endpoint_created']
    assert [(e['logical_id'], e['generation']) for e in created] == [
        ('w0', 1), ('r0', 1), ('w0', 2), ('w0', 3), ('r0', 2)]
    assert len({e['endpoint_gid'] for e in created}) == len({e['endpoint_generation'] for e in created}) == 5
    assert len({tuple(e[k] for k in ('pid', 'pgid', 'process_start_ticks', 'worker_instance_id', 'context_instance_id')) for e in created}) == 1
    empty = [e for e in rows if e['event'] == 'participant_group_snapshot']
    assert [e['command_id'] for e in empty] == [9, 10]
    assert all(e['endpoint_absence_observed'] and e['active_endpoint_count'] == 0 and e['endpoints'] == [] for e in empty)
    assert empty[0]['monotonic_ns'] <= empty[1]['monotonic_ns']
    assert empty[1]['monotonic_ns'] - empty[0]['monotonic_ns'] < 1_000_000_000
    sends = [e for e in rows if e['event'] == 'send']
    received = [e for e in rows if e['event'] == 'receive']
    assert [(e['generation'], e['send_sequence']) for e in sends] == [(2, 0), (3, 0)]
    assert [(e['generation'], e['payload_writer_generation'], e['send_sequence']) for e in received] == [(1, 2, 0), (2, 3, 0)]
    assert [json.loads(e['string_data'])['endpoint_gid'] for e in sends] == [created[2]['endpoint_gid'], created[3]['endpoint_gid']]


@pytest.mark.parametrize('layout,group_count', [('shared', 1), ('roles', 2), ('split', 4)])
def test_topology_writer_and_empty_interval_preserve_groups_and_new_delivery(tmp_path, transport, layout, group_count):
    from r2f.runtime.topology_pair import run_topology
    p = multi_program(2, 2, layout)
    p['actions'] += [
        action('retire_writer', 'destroy', 'w0', 1),
        action('siblings_live', 'snapshot'),
        action('writer_reborn', 'recreate', 'w0', 2),
        action('writer_reborn_send', 'publish_batch', 'w0', 2, count=1, start_sequence=0),
        action('retire_w0', 'destroy', 'w0', 2),
        action('retire_w1', 'destroy', 'w1', 1),
        action('retire_r0', 'destroy', 'r0', 1),
        action('retire_r1', 'destroy', 'r1', 1),
        action('empty_begin', 'snapshot'), action('empty_end', 'snapshot'),
        action('reborn_w0', 'recreate', 'w0', 3),
        action('reborn_w1', 'recreate', 'w1', 2),
        action('reborn_r0', 'recreate', 'r0', 2),
        action('reborn_r1', 'recreate', 'r1', 2),
        action('new_generation_send', 'publish_batch', 'w0', 3, count=1, start_sequence=0),
        action('new_generation_observe', 'snapshot'),
    ]
    assert model.validate_program(p)['status'] == 'valid'
    backend = prepared_backend(tmp_path/'carrier', p)
    backend.real_ros = False
    result = run_topology(p, dict(backend.config), tmp_path/'runtime', backend=backend)
    assert result['status'] != 'aborted', (result['reason'], result['unknowns'])
    applied = {e['action_id'] for e in result['events'] if e['event'] == 'topology_action_done'
               and e['receipt']['disposition'] == 'applied'}
    assert {'retire_writer', 'writer_reborn', 'new_generation_send'} <= applied
    lineage = result['execution_metadata']['participant_groups']
    assert 'error' not in lineage, lineage
    assert len(transport) == len(lineage['processes']) == len(lineage['contexts']) == group_count
    events = result['events']
    empty = [e for e in events if e['event'] == 'participant_group_snapshot' and e.get('endpoint_absence_observed')]
    # Every snapshot audits each already empty group, including during partial retirement.
    assert len(empty) >= 2 * group_count
    assert all(e['active_endpoint_count'] == 0 and e['endpoints'] == [] and 'logical_id' not in e for e in empty)
    receipts = {r['step_id']: r for r in result['observation_receipts'] if 'step_id' in r}
    assert all(len(receipts['snapshot:'+aid]['group_observations']) == group_count for aid in ('empty_begin', 'empty_end'))
    assert all(receipts['snapshot:'+aid]['complete'] for aid in ('empty_begin', 'empty_end'))
    created = [e for e in events if e['event'] == 'endpoint_created']
    assert len(created) == len({e['endpoint_gid'] for e in created}) == len({e['endpoint_generation'] for e in created}) == 9
    assert len({tuple(e[k] for k in ('pid', 'process_start_ticks', 'worker_instance_id', 'context_instance_id')) for e in created}) == group_count
    sends = [e for e in events if e['event'] == 'send']
    receives = [e for e in events if e['event'] == 'receive']
    assert [(e['generation'], e['send_sequence']) for e in sends] == [(2, 0), (3, 0)]
    assert [(e['logical_id'], e['generation'], e['payload_writer_generation']) for e in receives] == [
        ('r0', 1, 2), ('r1', 1, 2), ('r0', 2, 3), ('r1', 2, 3)]
    assert all(e['publisher_gid'] is None and e['publisher_gid_status'] == 'unavailable_api' for e in receives)
    assert not result['real_execution'] and not result['candidate_eligible'] and not result['sealed']

    # Empty-owner observations cannot start before the last destroy ACK closes.
    forged = deepcopy(events)
    first_empty = next(e for e in forged if e['event'] == 'participant_group_snapshot' and e.get('endpoint_absence_observed'))
    request = next(e for e in forged if e['event'] == 'group_command_requested'
                   and e['pid'] == first_empty['pid'] and e['command_id'] == first_empty['command_id'])
    destroy_ack = next(e for e in reversed(forged[:forged.index(request)])
                       if e['event'] == 'command_done' and e.get('command') == 'destroy_endpoint' and e['pid'] == first_empty['pid'])
    forged.remove(destroy_ack)
    forged.insert(forged.index(request)+1, destroy_ack)
    for i, e in enumerate(forged):
        e['event_index'] = i
    with pytest.raises(ValueError, match='group_lineage:'):
        project_lineage(p, forged, result['raw_path'], config=backend.config)


@pytest.mark.parametrize('transient', ['unbound', 'ambiguous'])
def test_worker_bootstrap_waits_for_delayed_own_graph_after_visible_sibling(monkeypatch, transient):
    from types import SimpleNamespace
    reads = []
    def graph_view(role, rows, entities):
        if role == 'writer' and len([e for e in entities if e.role == role]) == 2:
            reads.append(True)
            if len(reads) == 1:
                if transient == 'unbound':
                    return rows[:1]
                return rows + [SimpleNamespace(**{**vars(rows[-1]), 'endpoint_gid': bytes.fromhex('aa'*16)})]
        return rows
    rc, calls, events = worker_capture(monkeypatch, writers=2, readers=1, graph_view=graph_view)
    assert rc == 0, events[-1]
    created = [e for e in events if e['event'] == 'endpoint_created']
    assert [(e['logical_id'], e['endpoint_gid']) for e in created] == [
        ('w0', '0'*31+'1'), ('w1', '0'*31+'2'), ('r0', '0'*31+'3')]
    attempts = [e for e in events if e['event'] == 'endpoint_identity_bootstrap' and e['logical_id'] == 'w1']
    assert [e['candidate_count'] for e in attempts] == ([0, 1] if transient == 'unbound' else [2, 1])
    assert [e['phase'] for e in attempts] == ['attempt', 'attempt']
    assert attempts[0]['status'] == 'unknown' and attempts[-1]['status'] == 'bound'
    assert len(attempts[0]['graph']['publishers']) == (1 if transient == 'unbound' else 3)
    assert all(e['candidate_eligible'] is False for e in attempts)


@pytest.mark.parametrize('visibility,count', [('absent', 0), ('sibling_only', 0), ('multiple', 2)])
def test_worker_bootstrap_deadline_keeps_last_graph_unknown(monkeypatch, visibility, count):
    from itertools import count as ticks
    from types import SimpleNamespace
    from r2f.runtime import topology_worker as worker
    clock = ticks(0, .001)
    monkeypatch.setattr(worker.time, 'monotonic', lambda: next(clock))
    def configure(cfg, commands):
        cfg['startup_timeout_sec'] = .01
    def graph_view(role, rows, entities):
        if role == 'writer' and len([e for e in entities if e.role == role]) == 2:
            if visibility == 'absent':
                return []
            if visibility == 'sibling_only':
                return rows[:1]
            return rows + [SimpleNamespace(**{**vars(rows[-1]), 'endpoint_gid': bytes.fromhex('aa'*16)})]
        return rows
    rc, calls, events = worker_capture(monkeypatch, writers=2, readers=1,
                                      damage=configure, graph_view=graph_view)
    assert rc == 1
    assert not any(e['event'] == 'endpoint_created' and e['logical_id'] == 'w1' for e in events)
    diagnostics = [e for e in events if e['event'] == 'endpoint_identity_bootstrap' and e['logical_id'] == 'w1']
    final = diagnostics[-1]
    assert final['phase'] == 'final_failure' and final['status'] == 'unknown'
    assert final['reason'] == 'endpoint_gid_not_observed_within_startup_timeout'
    assert final['candidate_count'] == count and final['endpoint_gid'] is None
    assert final['graph'] == diagnostics[-2]['graph']
    assert 2 <= len(diagnostics) <= 12
    assert all(e['candidate_eligible'] is False for e in diagnostics)
    assert calls.count('destroy_writer') == 2 and calls.count('shutdown') == 1


@pytest.mark.parametrize('damage_kind,reason', [
    ('malformed', 'local_graph_endpoint_gid_invalid'),
    ('zero', 'local_graph_endpoint_gid_invalid'),
    ('duplicate', 'local_group_graph_identity_invalid'),
    ('missing_sibling', 'local_group_graph_identity_invalid'),
    ('malformed_ambiguous', 'local_graph_endpoint_gid_invalid'),
])
def test_worker_bootstrap_invalid_identity_fails_closed_with_raw_diagnostic(monkeypatch, damage_kind, reason):
    from types import SimpleNamespace
    def graph_view(role, rows, entities):
        if role != 'writer' or len([e for e in entities if e.role == role]) != 2:
            return rows
        if damage_kind == 'missing_sibling':
            return rows[1:]
        gid = bytes.fromhex('aa') if damage_kind.startswith('malformed') else bytes(16)
        if damage_kind == 'duplicate':
            gid = rows[0].endpoint_gid
        rows[-1] = SimpleNamespace(**{**vars(rows[-1]), 'endpoint_gid': gid})
        if damage_kind == 'malformed_ambiguous':
            rows.append(SimpleNamespace(**{**vars(rows[-1]), 'endpoint_gid': bytes.fromhex('bb'*16)}))
        return rows
    rc, calls, events = worker_capture(monkeypatch, writers=2, graph_view=graph_view)
    assert rc == 1
    diagnostics = [e for e in events if e['event'] == 'endpoint_identity_bootstrap' and e['logical_id'] == 'w1']
    assert [e['phase'] for e in diagnostics] == ['attempt', 'final_failure']
    assert all(e['status'] == 'error' and e['reason'] == reason and e['endpoint_gid'] is None
               and not e['candidate_eligible'] for e in diagnostics)
    assert diagnostics[-1]['graph'] == diagnostics[0]['graph']
    assert not any(e['event'] == 'endpoint_created' and e['logical_id'] == 'w1' for e in events)


@pytest.mark.parametrize('retired_only', [False, True])
def test_worker_bootstrap_never_binds_retired_gid_on_recreation(monkeypatch, retired_only):
    from itertools import count
    from types import SimpleNamespace
    from r2f.runtime import topology_worker as worker
    clock = count(0, .1)
    monkeypatch.setattr(worker.time, 'monotonic', lambda: next(clock))
    seen = []
    def graph_view(role, rows, entities):
        if role == 'reader' and rows and rows[0].endpoint_gid.hex() == '0'*31+'3':
            seen.append(True)
            if retired_only or len(seen) == 1:
                return [SimpleNamespace(**{**vars(rows[0]), 'endpoint_gid': bytes.fromhex('0'*31+'2')})]
        return rows
    rc, calls, events, _ = cp3_worker_capture(monkeypatch, graph_view=graph_view)
    assert rc == (1 if retired_only else 0)
    attempts = [e for e in events if e['event'] == 'endpoint_identity_bootstrap'
                and e['logical_id'] == 'r0' and e['generation'] == 2]
    assert attempts[0]['reason'] == 'local_graph_endpoint_gid_retired'
    assert attempts[0]['candidate_count'] == 1 and attempts[0]['status'] == 'unknown'
    assert attempts[0]['retired_gids'] == ['0'*31+'2']
    created = [e for e in events if e['event'] == 'endpoint_created' and e['logical_id'] == 'r0']
    assert [e['endpoint_gid'] for e in created] == (['0'*31+'2'] if retired_only else ['0'*31+'2', '0'*31+'3'])
    assert attempts[-1]['phase'] == ('final_failure' if retired_only else 'attempt')


def test_worker_rejects_identity_change_after_binding_without_bootstrap_retry(monkeypatch):
    from types import SimpleNamespace
    reads = []
    def graph_view(role, rows, entities):
        if role == 'reader' and rows:
            reads.append(True)
            if len(reads) > 2:
                return [SimpleNamespace(**{**vars(rows[0]), 'endpoint_gid': bytes.fromhex('ab'*16)})]
        return rows
    rc, calls, events, _ = cp3_worker_capture(monkeypatch, graph_view=graph_view)
    assert rc == 1
    assert any(e['event'] == 'worker_error' and 'unreceipted_endpoint_identity_change' in e.get('error', '') for e in events)
    created = [e for e in events if e['event'] == 'endpoint_created' and e['logical_id'] == 'r0']
    assert created and all(e['endpoint_gid'] in {'0'*31+'2', '0'*31+'3'} for e in created)
    assert not any(e['event'] == 'endpoint_identity_bootstrap' and e['status'] == 'unknown' for e in events)
