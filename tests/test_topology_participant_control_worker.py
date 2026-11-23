"""P2 public worker entry with labelled external ROS/native/OS fixtures.

No ROS/native libraries, process, proc read, socket, lock or signal is used.
"""
from contextlib import contextmanager
from types import SimpleNamespace, ModuleType
from unittest.mock import patch
import io
import json
import sys
import pytest
from r2f.runtime import topology_worker as worker
from test_topology_participant_control_input import controlled

@contextmanager
def ros_fixture(monkeypatch, commands, fail_reader=False, fail_destroy=False):
    calls=[]; entities=[]; queued=[]
    class Context:
        def __init__(self): calls.append('context')
        def get_domain_id(self): return 173
        def shutdown(self): calls.append('shutdown')
    class Executor:
        def __init__(self, **kw): pass
        def add_node(self,node): pass
        def spin_once(self,**kw):
            if queued:
                message=queued.pop(0)
                for e in entities:
                    if e.role=='reader': e.callback(message, {})
        def shutdown(self,**kw): calls.append('executor_shutdown')
    def qos_profile(**kw):
        defaults=dict(reliability='reliable',durability='volatile',history='keep_last',liveliness='automatic',depth=16,deadline_ns=9223372036854775807,liveliness_lease_duration_ns=9223372036854775807)
        defaults.update(kw)
        result=SimpleNamespace(**defaults)
        for name in ('reliability','durability','history','liveliness'):
            v=getattr(result,name); setattr(result,name,SimpleNamespace(name=str(getattr(v,'name',v))))
        result.deadline=SimpleNamespace(nanoseconds=defaults['deadline_ns'])
        result.liveliness_lease_duration=SimpleNamespace(nanoseconds=defaults['liveliness_lease_duration_ns'])
        return result
    class Endpoint:
        def __init__(self,role,topic,node,callback=None):
            self.role=role; self.topic_name=topic; self.node=node; self.callback=callback
            self.gid=('11' if role=='writer' else '22')*16
        def get_subscription_count(self): return sum(e.role=='reader' for e in entities)
        def get_publisher_count(self): return sum(e.role=='writer' for e in entities)
        def publish(self,message): queued.append(message)
    class Node:
        def __init__(self,name,**kw): calls.append('node');self.name=name
        def create_publisher(self,typ,topic,qos,**kw):
            calls.append('writer'); e=Endpoint('writer',topic,self); entities.append(e);return e
        def create_subscription(self,typ,topic,cb,qos,**kw):
            calls.append('reader')
            if fail_reader: raise RuntimeError('fixture_reader_create_failure')
            e=Endpoint('reader',topic,self,cb);entities.append(e);return e
        def rows(self,role):
            return [SimpleNamespace(node_name=e.node.name,node_namespace='/',topic_type='std_msgs/msg/String',endpoint_gid=bytes.fromhex(e.gid),qos_profile=qos_profile()) for e in entities if e.role==role]
        def get_publishers_info_by_topic(self,topic): return self.rows('writer')
        def get_subscriptions_info_by_topic(self,topic): return self.rows('reader')
        def destroy_publisher(self,e): calls.append('destroy_writer'); entities.remove(e);return not fail_destroy
        def destroy_subscription(self,e): calls.append('destroy_reader'); entities.remove(e);return True
        def destroy_node(self): calls.append('destroy_node'); entities.clear()
    modules={}
    for name in ('rclpy','rclpy.context','rclpy.executors','rclpy.node','rclpy.qos','rclpy.duration','std_msgs','std_msgs.msg'):
        modules[name]=ModuleType(name)
    modules['rclpy'].init=lambda **kw: calls.append('init')
    modules['rclpy'].get_rmw_implementation_identifier=lambda:'rmw_cyclonedds_cpp'
    modules['rclpy.context'].Context=Context
    modules['rclpy.executors'].SingleThreadedExecutor=Executor
    modules['rclpy.node'].Node=Node
    modules['rclpy.qos'].QoSProfile=qos_profile
    modules['rclpy.duration'].Duration=lambda **kw:SimpleNamespace(**kw)
    from enum import Enum
    for name,values in [('DurabilityPolicy','VOLATILE TRANSIENT_LOCAL'),('HistoryPolicy','KEEP_LAST KEEP_ALL'),('LivelinessPolicy','AUTOMATIC MANUAL_BY_TOPIC'),('ReliabilityPolicy','RELIABLE BEST_EFFORT')]:
        setattr(modules['rclpy.qos'],name,Enum(name,values))
    modules['std_msgs.msg'].String=type('String',(),{})
    for k,v in modules.items(): monkeypatch.setitem(sys.modules,k,v)
    activation=ModuleType('r2f.instrumentation.fastdds_peer.activation')
    activation.reference_from_config=lambda cfg:None
    activation.manifest_for_reference=lambda ref:None
    activation.verify_vendor_mapping=lambda ref:None
    monkeypatch.setitem(sys.modules,activation.__name__,activation)
    # Native readback and publish are external ABI boundaries, explicitly fixture.
    actual=ModuleType('r2f.runtime.actual_qos')
    pub=ModuleType('r2f.runtime.rmw_publish_observer')
    monkeypatch.setitem(sys.modules,actual.__name__,actual)
    monkeypatch.setitem(sys.modules,pub.__name__,pub)
    from r2f.runtime.topology_worker import endpoint_expressibility
    q=endpoint_expressibility(controlled()['endpoints'][0],{})['public_qos']
    actual.read_public_endpoint_qos=lambda *a:{'qos':q,'fixture':True}
    class Publish:
        def __init__(self,*a): pass
        def publish(self,e,m,**kw): e.publish(m);return {'fixture':True}
        def close(self): return {'fixture':True,'closed':True}
    pub.PublishOccurrenceObserver=Publish
    # Source identity, kernel IDs, clocks and stdin are fixture boundaries only.
    monkeypatch.setattr(worker,'_monotonic_clock_identity',lambda:{'fixture':True})
    original=worker.Path.read_text
    def read(path,*a,**kw):
        if str(path)=='/proc/self/stat': return '100 (fixture) '+' '.join(['0']*19+['900'])
        return original(path,*a,**kw)
    monkeypatch.setattr(worker.Path,'read_text',read)
    monkeypatch.setattr(worker.os,'getpid',lambda:100)
    monkeypatch.setattr(worker.os,'getpgrp',lambda:100)
    class Input:
        def fileno(self): return 77
    monkeypatch.setattr(sys,'stdin',Input())
    chunks=iter([json.dumps(c).encode()+b'\n' for c in commands]+[b''])
    monkeypatch.setattr(worker.os,'read',lambda *a:next(chunks,b''))
    import select
    monkeypatch.setattr(select,'select',lambda *a:([sys.stdin],[],[]))
    stream=io.StringIO()
    with patch('sys.stdout',stream):
        yield calls,entities,stream


def group_config():
    p=controlled()
    return {'run_id':'offline-fixture','domain_id':173,'rmw':'rmw_cyclonedds_cpp',
            'source_repo':'/fixture','participant_control':p['participant_control'],
            'group_id':'group_a','group_members':p['endpoints'],
            'worker_instance_id':'process-fixture','context_instance_id':'context-fixture',
            'logical_id':'w0','generation':1}

def create(eid,serial):
    return {'command':'create_endpoint','command_id':serial,
            'logical_id':eid,'generation':1,'worker_role':'writer' if eid=='w0' else 'reader'}

def run_worker(monkeypatch,commands,fail_reader=False):
    cfg=group_config()
    with ros_fixture(monkeypatch,commands,fail_reader) as (calls,entities,stream):
        monkeypatch.setattr(sys,'argv',['worker',json.dumps(cfg),'writer'])
        rc=worker.worker_main()
    return rc,calls,[json.loads(line) for line in stream.getvalue().splitlines()]

def test_public_worker_lazily_creates_two_sessions_in_one_context(monkeypatch):
    rc,calls,events=run_worker(monkeypatch,[create('r0',1),create('w0',2),{'command':'stop','command_id':3}])
    assert rc==0,events
    assert calls.count('context')==calls.count('init')==calls.count('node')==1
    assert [e['logical_id'] for e in events if e['event']=='endpoint_created']==['r0','w0']
    tokens=[e for e in events if e['event']=='endpoint_created']
    assert len({e['endpoint_generation'] for e in tokens})==2
    assert all(e['endpoint_generation']!=e['context_instance_id'] for e in tokens)
    assert calls.count('shutdown')==calls.count('destroy_node')==1
    assert calls.count('destroy_writer')==calls.count('destroy_reader')==1

def test_partial_create_failure_still_closes_first_endpoint_and_context(monkeypatch):
    rc,calls,events=run_worker(monkeypatch,[create('w0',1),create('r0',2)],fail_reader=True)
    assert rc==1
    assert calls.count('destroy_writer')==1
    assert calls.count('destroy_node')==calls.count('shutdown')==1
    assert 'fixture_reader_create_failure' in events[-1]['error']

@pytest.mark.parametrize('bad', ['reuse','wrong_role','wrong_generation','serial','stale_nonce'])
def test_group_address_and_reuse_fail_closed_with_final_cleanup(monkeypatch,bad):
    cmd=create('w0',2)
    if bad=='wrong_role': cmd=create('r0',2);cmd['worker_role']='writer'
    elif bad=='wrong_generation': cmd=create('r0',2);cmd['generation']=2
    elif bad=='serial': cmd=create('r0',1)
    elif bad=='stale_nonce': cmd={'command':'snapshot','logical_id':'w0','generation':1,'endpoint_generation':'stale','command_id':2}
    rc,calls,events=run_worker(monkeypatch,[create('w0',1),cmd])
    assert rc==1,events
    assert calls.count('destroy_writer')==1
    assert calls.count('shutdown')==1


def test_callback_after_second_create_keeps_reader_identity(monkeypatch):
    # Choose endpoint nonces via the external UUID boundary, not session internals.
    from itertools import count
    n=count(1)
    monkeypatch.setattr(worker.uuid,'uuid4',lambda:SimpleNamespace(hex=f'{next(n):032x}'))
    cmds=[create('r0',1),create('w0',2),
          {'command':'publish_batch','command_id':3,'logical_id':'w0','generation':1,
           'endpoint_generation':f'{3:032x}','sequences':[7,8],'phase':'fixture','interval_sec':0},
          {'command':'stop','command_id':4}]
    rc,calls,events=run_worker(monkeypatch,cmds)
    assert rc==0,events
    received=[e for e in events if e['event']=='receive']
    assert received and all(e['logical_id']=='r0' and e['role']=='reader'
                            and e['endpoint_generation']==f'{2:032x}' and e['reader_gid']=='22'*16 for e in received)
    assert all(e['logical_id']=='w0' and e['endpoint_generation']==f'{3:032x}' for e in events if e['event']=='send')

@pytest.fixture
def transport(monkeypatch, tmp_path):
    """Labelled OS process/pipe/selector/lease doubles, never real spawn/locks."""
    from copy import deepcopy
    from itertools import count
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
                'run_id':'offline-fixture','domain_observation_source':'fixture_api','monotonic_ns':1000,
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
                token=dict(logical_id=eid,generation=1,endpoint_generation=f'fixture-{eid}',
                    endpoint_gid=('11' if spec['role']=='writer' else '22')*16,
                    role=spec['role'],worker_role=spec['role'])
                self.tokens[eid]=token
                from r2f.runtime.topology_worker import endpoint_expressibility
                q=endpoint_expressibility(spec,{})['public_qos']
                self.add('endpoint_created',**token,topic=spec['topic'],message_type=spec['type_name'],qos=q,
                    node_name='fixture-group-node',created_monotonic_ns=100,
                    identity_source='get_publishers_info_by_topic' if spec['role']=='writer' else 'get_subscriptions_info_by_topic')
                self.add('endpoint_group_bound',endpoint={k:token[k] for k in ('logical_id','generation','endpoint_generation','endpoint_gid','worker_role')})
            token=deepcopy(self.tokens[eid])
            if op=='snapshot':
                self.add('match_snapshot',**token,command_id=cid,matched_count=1)
                pubs=[dict(endpoint_gid=t['endpoint_gid'],node_name='fixture-group-node',topic_type='std_msgs/msg/String',qos={}) for t in self.tokens.values() if t['role']=='writer']
                subs=[dict(endpoint_gid=t['endpoint_gid'],node_name='fixture-group-node',topic_type='std_msgs/msg/String',qos={}) for t in self.tokens.values() if t['role']=='reader']
                self.add('graph_snapshot',**token,command_id=cid,publishers=pubs,subscriptions=subs)
            elif op=='publish_batch':
                for sequence in c['sequences']:
                    self.add('send',**token,command_id=cid,send_sequence=sequence,writer_gid=token['endpoint_gid'])
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


def prepared_backend(tmp_path, program):
    backend=worker.RosTopologyBackend({'run_id':'offline-fixture','domain_id':173,
        'rmw':'rmw_cyclonedds_cpp','source_repo':'/fixture','live_execution_go':True,
        'snapshot_timeout_sec':0.1,'publish_timeout_sec':0.1},tmp_path)
    backend.execution_preflight={'ok':True,'fixture':True}
    backend.execution_environment={'fixture':'only'}
    receipt=backend.prepare(program)
    assert receipt['ok']
    return backend

@pytest.mark.parametrize('shared',[True,False])
def test_public_backend_uses_one_transport_per_group_and_addresses_snapshots(monkeypatch,tmp_path,transport,shared):
    from test_topology_participant_control_input import controlled
    p=controlled(shared);backend=prepared_backend(tmp_path,p)
    writer=backend.create_endpoint(p['endpoints'][0],1,'writer')
    assert len(transport)==1 and len(transport[0].tokens)==1
    reader=backend.create_endpoint(p['endpoints'][1],1,'reader')
    assert len(transport)==(1 if shared else 2)
    assert (writer['process'] is reader['process']) is shared
    snapshot=backend.snapshot({'w0':writer,'r0':reader})
    assert snapshot['complete'] and set(snapshot['snapshots'])=={'w0','r0'}
    backend.publish_batch(writer,[1,2],0,'fixture')
    closed=backend.close()
    assert closed['closed'] and len(closed['workers'])==(1 if shared else 2)
    assert not writer['alive'] and not reader['alive']
    assert backend.close()['already_closed']
    assert len([e for process in backend.processes for e in process.events if e['event']=='endpoint_created'])==2


def test_public_backend_rejects_cross_address_ack_and_cleans_entire_group(monkeypatch,tmp_path,transport):
    p=controlled();backend=prepared_backend(tmp_path,p)
    writer=backend.create_endpoint(p['endpoints'][0],1,'writer')
    reader=backend.create_endpoint(p['endpoints'][1],1,'reader')
    transport[0].bad_ack=True
    with pytest.raises(worker.TopologyBackendError):backend.snapshot({'w0':writer})
    closed=backend.close()
    # The persistent forged ACK also invalidates final capture, while shutdown
    # must still close the actual process and both endpoint sessions.
    assert not closed['closed'] and not closed['survivors']
    assert all(w['closed'] for w in closed['workers'])
    assert any('final_group_capture' in e['detail'] for e in closed['errors'])
    assert not writer['alive'] and not reader['alive']

@pytest.mark.parametrize('key,value',[('rmw','rmw_fastrtps_cpp'),('environment_mutation',{'kind':'discovery_netem'}),('observation_mode','instrumentation')])
def test_public_prepare_refuses_unsupported_control_before_environment(monkeypatch,tmp_path,key,value):
    b=worker.RosTopologyBackend({'live_execution_go':True,key:value},tmp_path)
    with pytest.raises(worker.TopologyUnsupported,match='participant_control_scope'):
        b.prepare(controlled())

@pytest.mark.parametrize('roots,missing,damage',[(1,False,None),(2,False,None),(1,True,None),
    *[(1,False,d) for d in ('provenance','root_self','root_parent','root_guid','sample_root_identity','sample_role')]])
def test_public_worker_group_capture_uses_observed_root_inventory_not_labels(monkeypatch,roots,missing,damage):
    from copy import deepcopy
    from itertools import count
    n=count(1);monkeypatch.setattr(worker.uuid,'uuid4',lambda:SimpleNamespace(hex=f'{next(n):032x}'))
    class NativeFixture:
        """External Cyclone ABI double: this is not a native observation."""
        def sample_roots(self,domain):
            return {'complete':True,'domain_id':domain,'count':roots,'provenance':{'fixture':True,'not_native':True},
                    'roots':[{'participant_handle':10+i,'participant_gid':('x'*32 if damage=='root_guid' else f'{90+i:032x}'),
                              'domain_id':domain,'domain_return_code':0,'parent':(999 if damage=='root_parent' else 0),
                              'participant_self_return':(999 if damage=='root_self' else 10+i),'valid':True} for i in range(roots)]}
        def sample_ownership(self,domain,gid,kind):
            if missing:return {'schema':'r2f.cyclone-participant-ownership/v2','complete':False,'status':'unknown'}
            h=20 if kind=='writer' else 30
            return {'schema':'r2f.cyclone-participant-ownership/v2','complete':True,'status':'observed','candidate_eligible':False,
                'stable_sample_count':2,'domain_id':domain,'local_gid':gid,'role_kind':('alien' if damage=='sample_role' else kind),'local_handle':h,
                'participant_handle':10,'participant_gid':f'{90:032x}','parent_chain':[h,10],
                'entity_inventory':[[10,[h]],[h,[]]],
                'entity_identities':[{'handle':h,'gid':gid},{'handle':10,'gid':('a'*32 if damage=='sample_root_identity' else f'{90:032x}')}],
                'participant_root_observation':{'participant_handle':10,'participant_self_return':10,'lookup_participants':[10]},
                'provenance':({} if damage=='provenance' else {'fixture':True,'not_native':True})}
    monkeypatch.setattr(worker,'CycloneMatchedPeerObserver',NativeFixture)
    cmds=[create('w0',1),create('r0',2),{'command':'group_snapshot','command_id':3,'logical_id':'w0','generation':1,
        'endpoint_generation':f'{2:032x}'},{'command':'stop','command_id':4}]
    rc,calls,events=run_worker(monkeypatch,cmds)
    assert rc==0,events
    observation=next(e for e in events if e['event']=='participant_group_snapshot')
    assert observation['actual_local_root_count']==(None if damage in {'root_self','root_parent','root_guid'} else roots)
    assert observation['group_relation_observed'] is (roots==1 and not missing and damage is None)
    assert not observation['candidate_eligible'] and observation['supporting_diagnostic']
    assert len(observation['endpoints'])==2
    assert all(len(e['samples'])==2 for e in observation['endpoints'])

def test_public_backend_captures_groups_before_first_publication(monkeypatch,tmp_path,transport):
    p=controlled();backend=prepared_backend(tmp_path,p)
    w=backend.create_endpoint(p['endpoints'][0],1,'writer')
    backend.create_endpoint(p['endpoints'][1],1,'reader')
    backend.publish_batch(w,[1],0,'fixture')
    events=backend.processes[0].events
    names=[e['event'] for e in events]
    assert 'participant_group_snapshot' in names
    assert names.index('participant_group_snapshot')<names.index('send')
    assert backend.close()['closed']

def test_public_backend_final_cleanup_captures_roots_without_authored_snapshot(monkeypatch,tmp_path,transport):
    p=controlled();backend=prepared_backend(tmp_path,p)
    backend.create_endpoint(p['endpoints'][0],1,'writer')
    backend.create_endpoint(p['endpoints'][1],1,'reader')
    assert backend.close()['closed']
    events=backend.processes[0].events
    names=[e['event'] for e in events]
    assert 'participant_group_snapshot' in names
    assert names.index('participant_group_snapshot')<names.index('group_context_shutdown')

def test_public_backend_enforces_authored_create_order_before_allocating(monkeypatch,tmp_path,transport):
    p=controlled();backend=prepared_backend(tmp_path,p)
    with pytest.raises(worker.TopologyBackendError,match='group_endpoint_create_order'):
        backend.create_endpoint(p['endpoints'][1],1,'reader')
    assert transport==[]
    assert backend.close()['closed']

def test_public_backend_rejects_forged_group_process_pgid(monkeypatch,tmp_path,transport):
    p=controlled();backend=prepared_backend(tmp_path,p)
    backend.create_endpoint(p['endpoints'][0],1,'writer')
    child=transport[0];original=type(child).add
    def forged(self,event,**fields):
        if event=='endpoint_created':fields['pgid']=self.pid+100
        return original(self,event,**fields)
    monkeypatch.setattr(type(child),'add',forged)
    with pytest.raises(worker.TopologyBackendError):
        backend.create_endpoint(p['endpoints'][1],1,'reader')
    assert child.poll()==0


def test_public_worker_preserves_endpoint_cleanup_failure_and_closes_owner(monkeypatch):
    with ros_fixture(monkeypatch,[create('w0',1),create('r0',2),{'command':'stop','command_id':3}],fail_destroy=True) as (calls,entities,output):
        monkeypatch.setattr(sys,'argv',['worker',json.dumps(group_config()),'writer'])
        rc=worker.worker_main()
    events=[json.loads(line) for line in output.getvalue().splitlines()]
    assert rc==1
    assert calls.count('destroy_reader')==calls.count('destroy_node')==calls.count('shutdown')==1
    closed=next(e for e in events if e['event']=='group_context_shutdown')
    assert not closed['closed'] and closed['errors']
    assert any(e['event']=='endpoint_destroyed' and e.get('cleanup_status')=='unknown' for e in events)
