"""Fourth-target evidence adapter. Wire parsing and reconciliation stay independent.

Only immutable normal-public-API receipts are consumed; lifecycle and admission
remain core responsibilities. Controlled projections never alter original sources.
"""
from copy import deepcopy
import json
from pathlib import Path
from r2f.core.evidence import verify_raw, canonical_hash
from r2f.core.state_machine import StepReceipt
from r2f.instrumentation.sedp import parse_sedp_capture, reconcile_discovery_projection, check_offered_requested
from .runtime_adapter import _one, _iso
from .recovery_adapter import _isolation, _step_evidence

TARGET = 'builtin.discovery_projection_reconciliation'


def _json(ref):
    path, sha = ref.get('path', ref.get('raw_path')), ref.get('sha256', ref.get('raw_sha256'))
    verify_raw(path, sha)
    return json.loads(Path(path).read_text())


def _lifecycle(events, runtime, config, observation):
    bindings = observation['endpoint_bindings']
    if len(bindings) != 2 or {b.get('role') for b in bindings} != {'writer', 'reader'}:
        raise ValueError('projection requires exactly one writer and reader binding')
    begin = _one(events, 'mutation_begin' if config['arm'] == 'mutated' else 'control_begin')
    tokens = {}
    for binding in bindings:
        role = binding['role']
        call, returned = _one(events, 'endpoint_create_begin', role=role), _one(events, 'endpoint_created', role=role)
        guid = returned.get('endpoint_gid')
        if isinstance(guid, str) and len(guid) == 48 and guid[32:] == '0'*16:
            guid = guid[:32]
        if guid != binding['endpoint_guid']:
            raise ValueError('projection lifecycle GUID differs from binding')
        for label, event in (('begin_raw_ref', call), ('return_raw_ref', returned)):
            ref = binding[label]
            if (ref.get('path') != runtime['raw_path'] or ref.get('sha256') != runtime['raw_sha256']
                    or ref.get('event_index') != event['event_index']):
                raise ValueError('projection lifecycle raw reference differs')
            for key in ('pid', 'process_start_ticks', 'role'):
                if binding[key] != event.get(key):
                    raise ValueError('projection lifecycle process differs:'+key)
            if binding['generation'] != event.get('generation', event.get('endpoint_generation')):
                raise ValueError('projection lifecycle generation differs')
        expected_qos = config.get(role+'_qos', config['qos'])
        if any(e.get('qos') != expected_qos or e.get('topic') != config['topic'] or e.get('message_type') != config['message_type'] for e in (call, returned)):
            raise ValueError('actual create-call input differs from capsule')
        if not begin['monotonic_ns'] < call['monotonic_ns'] <= returned['monotonic_ns']:
            raise ValueError('projection endpoint creation preceded mutation begin')
        tokens[role] = {'id':guid, 'generation':binding['generation'], 'created_ns':returned['monotonic_ns'],
                        'pid':binding['pid'], 'process_start_ticks':binding['process_start_ticks']}
    if tokens['writer']['pid'] == tokens['reader']['pid']:
        raise ValueError('projection lacks two independent endpoint workers')
    return tokens


def projection_receipts(scenario, runtime, events):
    env, locks = _one(events, 'environment_preflight'), _one(events, 'isolation_locks_acquired')
    admission = _one(events, 'namespace_host_admission_verified')
    _isolation(admission['isolation'])
    ready = _one(events, 'discovery_observer_ready')
    ns_ready = _one(events,'namespace_ready')
    if (ns_ready.get('ready') is not True or ns_ready.get('drop_installed') is not False
            or ns_ready.get('namespace_rtps_probe',{}).get('ok') is not True
            or ns_ready.get('isolation') != admission['isolation']
            or not admission['event_index'] < ns_ready['event_index'] < ready['event_index']):
        raise ValueError('projection namespace readiness proof incomplete')
    state = {'environment_ok':env.get('ok') is True, 'locks_held':bool(locks.get('locks'))}
    actuals = runtime.get('precondition_receipts', [])
    if len(actuals) != len(scenario.state_steps):
        raise ValueError('projection precondition count differs from target')
    receipts = []
    for step, actual in zip(scenario.state_steps, actuals):
        if actual.get('step_id') != step.step_id or actual.get('requires') != [c.condition_id for c in step.requires]:
            raise ValueError('projection runtime step or requires differs')
        before, after = _step_evidence(events, actual)
        proof_event = ns_ready if step.step_id == 'namespace_ready' else ready
        if not proof_event['monotonic_ns'] <= after['monotonic_ns'] or proof_event['event_index'] >= after['event_index']:
            raise ValueError('projection precondition proof postdates reached')
        if step.step_id == 'observer_ready':
            receipt = ready.get('receipt', {})
            actual_ready = ready.get('ready', receipt.get('ready', {}))
            if not isinstance(actual_ready,dict) or actual_ready.get('status') != 'ready' or admission['event_index'] >= ready['event_index']:
                raise ValueError('independent capture readiness missing')
        checks = {c.condition_id:c.evaluate(state) for c in step.requires}
        if not all(checks.values()):
            raise ValueError('projection precondition requires not satisfied')
        snapshot = dict(state); state[step.step_id] = True
        result = {'runtime_step_id':step.step_id, 'raw_before_index':before['event_index'],
                  'raw_after_index':after['event_index'], 'runtime_receipt':actual, 'admission_raw_index':proof_event['event_index']}
        receipts.append(StepReceipt(step.step_id, step.action, snapshot, dict(state), checks, 'executed',
                                    _iso(before['time_ns']), _iso(after['time_ns']), result).to_dict())
    begin = _one(events, 'mutation_begin' if scenario.arm == 'mutated' else 'control_begin')
    if begin['monotonic_ns'] <= after['monotonic_ns']:
        raise ValueError('projection mutation began before preconditions')
    snapshot = begin.get('preconditions_snapshot', {})
    if not all(snapshot.get(k) is True for k in ('namespace_ready','observer_ready')) or snapshot.get('endpoints_created') is not False:
        raise ValueError('projection begin lacks genuine pre-create snapshot')
    refs = [{'step_id':r['step_id'], 'raw_before_index':r['result']['raw_before_index'], 'raw_after_index':r['result']['raw_after_index']} for r in receipts]
    if begin.get('precondition_receipt_refs') != refs:
        raise ValueError('projection begin precondition refs differ')
    if any(e.get('event') in ('endpoint_create_begin','endpoint_created') and e['monotonic_ns'] <= begin['monotonic_ns'] for e in events):
        raise ValueError('projection endpoint existed before mutation began')
    return receipts


def projection_graph_proof(events, config, observation, tokens):
    exported = _one(events, 'projection_graph_exported')
    if exported.get('graph_snapshot') != observation['graph_snapshot']:
        raise ValueError('projection graph export differs from raw')
    proofs = exported.get('graph_receipts', [])
    if len(proofs) != 2 or {p.get('role') for p in proofs} != {'writer','reader'}:
        raise ValueError('projection graph requires both command chains')
    snapshots = {}
    for proof in proofs:
        role=proof['role']; token=tokens[role]
        request, match, graph, ack = [events[proof[k+'_index']] for k in ('request','match','snapshot','ack')]
        if [e.get('event') for e in (request,match,graph,ack)] != ['graph_snapshot_requested','match_snapshot','graph_snapshot','command_done']:
            raise ValueError('projection graph command chain event differs')
        command=proof['command_id']
        chain=[e for e in events if e.get('command_id')==command and e.get('role')==role and e.get('event') in ('graph_snapshot_requested','match_snapshot','graph_snapshot','command_done')]
        if chain != [request,match,graph,ack] or any(e.get('command_id')!=command or e.get('role')!=role for e in chain):
            raise ValueError('projection graph command identity is ambiguous')
        stamps=[request['requested_monotonic_ns'],match['sampled_monotonic_ns'],match['monotonic_ns'],graph['monotonic_ns'],ack['monotonic_ns'],exported['monotonic_ns']]
        if any(type(t)is not int for t in stamps) or stamps!=sorted(stamps) or stamps[0] < token['created_ns']:
            raise ValueError('projection graph sample/ACK chronology invalid')
        for event in (match,graph,ack):
            if event.get('pid')!=token['pid'] or event.get('endpoint_generation',event.get('generation'))!=token['generation']:
                raise ValueError('projection graph worker generation differs')
        if match.get('count_api') != ('get_subscription_count' if role=='writer' else 'get_publisher_count') or type(match.get('matched_count'))is not int or match['matched_count']<0:
            raise ValueError('projection count diagnostic not a real public snapshot')
        # Count is diagnostic; zero never becomes a new primary violation.
        for key,owner in (('publishers','writer'),('subscriptions','reader')):
            items=graph[key]
            if len(items)!=1 or items[0]['endpoint_gid'][:32]!=tokens[owner]['id']:
                raise ValueError('projection graph is not the exact endpoint pair')
        snapshots[role]=graph
    graph=observation['graph_snapshot']; content=_json(graph['raw'])
    expected={'source':'ros_graph','captured_at_unix_ns':snapshots['writer']['time_ns'],'endpoints':[]}
    for key,role in (('publishers','writer'),('subscriptions','reader')):
        for item in snapshots['writer'][key]:
            expected['endpoints'].append({'endpoint_gid':item['endpoint_gid'],'role':role,'topic':config['topic'],
                'topic_type':item['topic_type'],'qos':deepcopy(item['qos'])})
    if content != expected or {k:v for k,v in graph.items() if k!='raw'} != expected:
        raise ValueError('independent graph JSON not reproduced by public graph raw')
    begin=_one(events,'mutation_begin' if config['arm']=='mutated' else 'control_begin')
    close=_one(events,'window_close',window='endpoint_creation')
    window=observation['observation_window']
    if window != {'start_ns':begin['time_ns'],'end_ns':exported['time_ns']} or not exported['monotonic_ns'] <= close['monotonic_ns']:
        raise ValueError('projection observation window differs from raw')
    offsets=[e['time_ns']-e['monotonic_ns'] for e in (begin,exported,close)]
    if max(offsets)-min(offsets)>50000000:
        raise ValueError('projection observation clock discontinuity')
    return {p['role']:events[p['match_index']]['matched_count'] for p in proofs}


def projection_fault_receipt(events, config, observation):
    receipt = deepcopy(_one(events, 'fault_control_receipt'))
    receipt['raw_event_ref'] = receipt['event_index']
    if receipt.get('schema', 'r2f.fault-control-receipt/v1') != 'r2f.fault-control-receipt/v1':
        raise ValueError('projection fault schema differs')
    receipt['schema'] = 'r2f.fault-control-receipt/v1'
    required = {'fault_id':'controlled_projection_field_mismatch','receipt_kind':'projection_field_mismatch',
                'applied':True,'responsibility_layer':'harness','candidate_eligible':False,'projection_source':'graph'}
    if config['arm'] != 'negative' or any(type(receipt.get(k)) is not type(v) or receipt.get(k) != v for k,v in required.items()):
        raise ValueError('projection fault receipt is not an independent harness control')
    graph = observation['graph_snapshot']; capture = observation['capture_receipt']
    original = {'builtin':{'raw_ref':capture['raw']['path'],'raw_sha256':capture['raw']['sha256']},
                'graph':{'raw_ref':graph['raw']['path'],'raw_sha256':graph['raw']['sha256']}}
    if receipt.get('original_sources') != original:
        raise ValueError('projection control original sources differ')
    controlled = _json({'path':receipt['controlled_projection_ref'], 'sha256':receipt['controlled_projection_sha256']})
    actual = _json(graph['raw'])
    expected = deepcopy(actual)
    path = receipt['field_path'].split('/')
    if len(path) != 5 or path[0] != '' or path[1] != 'endpoints' or path[3:] != ['qos','reliability']:
        raise ValueError('projection control field outside declared reliability diagnostic')
    index = int(path[2]); endpoint = expected['endpoints'][index]
    if endpoint.get('role') != 'writer' or endpoint['qos']['reliability'] != receipt['before'] or canonical_hash(receipt['before']) == canonical_hash(receipt['after']):
        raise ValueError('projection control before/after identity differs')
    endpoint['qos']['reliability'] = receipt['after']
    if expected != controlled:
        raise ValueError('projection control changed more than one declared field')
    if Path(receipt['controlled_projection_ref']).resolve() in {Path(v['raw_ref']).resolve() for v in original.values()}:
        raise ValueError('projection control overwrote original source')
    return {k:v for k,v in receipt.items() if k not in ('event','event_index','run_id','domain_id','time_ns','monotonic_ns','pid')}


def projection_facts(events, runtime, config):
    facts = {'schema_version':1, 'environment_healthy':runtime.get('environment',{}).get('ok') is True,
             'process_healthy':False,'preconditions_reached':False,'endpoint_identity_complete':False}
    try:
        observation = runtime['discovery_projection']
        raw_observation = _one(events,'projection_observation')
        for key in ('capture_receipt','graph_snapshot','observation_window','field_policy'):
            if raw_observation.get(key) != observation.get(key):
                raise ValueError('projection raw observation differs:'+key)
        tokens = _lifecycle(events,runtime,config,observation)
        facts.update(tokens, endpoint_identity_complete=True)
        counts=projection_graph_proof(events,config,observation,tokens)
        ready=_one(events,'discovery_observer_ready')
        stopped=_one(events,'discovery_observer_stopped')
        if observation['capture_receipt'].get('ready') != ready.get('ready') or stopped.get('receipt') != observation['capture_receipt']:
            raise ValueError('projection capture ready/stop receipt differs from raw')
        # This call always starts at independently captured packets; no graph or
        # submitted configuration is accepted as input to the wire parser.
        discovery = parse_sedp_capture(observation['capture_receipt'],field_policy=observation.get('field_policy'))
        kwargs = {k:observation[k] for k in ('endpoint_bindings','observation_window')}
        kwargs['field_policy'] = observation.get('field_policy')
        original = reconcile_discovery_projection(discovery,observation['graph_snapshot'],**kwargs)
        result = original
        if config['arm']=='negative':
            receipt = projection_fault_receipt(events,config,observation)
            if original['comparison_status'] != 'consistent':
                raise ValueError('projection fault original source comparison not healthy')
            controlled = _json({'path':receipt['controlled_projection_ref'],'sha256':receipt['controlled_projection_sha256']})
            controlled['raw'] = {'path':receipt['controlled_projection_ref'],'sha256':receipt['controlled_projection_sha256']}
            result = reconcile_discovery_projection(discovery,controlled,**kwargs)
            if result['comparison_status'] != 'mismatch' or len(result['differences']) != 1 or result['differences'][0]['field'] != 'reliability':
                raise ValueError('known projection fault did not isolate one primary mismatch')
        workers = (runtime.get('execution_metadata') or runtime.get('execution') or {}).get('workers',[])
        if isinstance(workers,dict): workers=list(workers.values())
        healthy = (len(workers)==2 and {w.get('role') for w in workers}=={'writer','reader'}
                   and all(w.get('returncode')==0 and w.get('pid')==tokens[w['role']]['pid'] for w in workers)
                   and _one(events,'namespace_controller_exited').get('returncode')==0
                   and runtime.get('runtime_status')=='completed'
                   and runtime.get('carrier_implementation')=='isolated_netns_passive_sedp_public_qos'
                   and not any(e.get('event') in ('worker_error','worker_unexpected_exit','runtime_abort','owned_worker_cleanup_signal') for e in events))
        window = observation['observation_window']
        observed_endpoints={role:next((e for e in reversed(discovery['events']) if e.get('endpoint_guid')==token['id'] and e.get('lifecycle')=='alive'),{}) for role,token in tokens.items()}
        compatibility=check_offered_requested(observed_endpoints['writer'],observed_endpoints['reader'])
        facts.update(projection=result, original_projection=original,
                     window={'start_ns':window['start_ns'],'end_ns':window['end_ns'],'complete':not result['unknown_reasons']},
                     process_healthy=healthy,preconditions_reached=True,
                     observed_effects=['expected_qos_projection'] if not original['unknown_reasons'] else [],
                     diagnostics={'actual_match':{'counts':counts,'observed':all(c==1 for c in counts.values()),'finite_no_match':'unknown'},
                                  'offered_requested':compatibility})
        # The actual public create-call receipts bind submitted QoS. A graph
        # value disagreeing with wire is the primary observation, not a reason
        # to abort and discard a potential SUT projection inconsistency.
        if config['arm']=='mutated' and config['qos']==config['baseline_qos']:
            # A legitimate submitted input that equals the neutral baseline
            # has no observable mutation effect.  Record it as ineffective so
            # the lifecycle reports mutation_ineffective, never a SUT finding.
            facts['mutation_ineffective'] = True
            facts['mutation_ineffective_reason'] = 'qos_equals_baseline'
    except (KeyError,ValueError,TypeError,OSError,IndexError) as exc:
        facts['evidence_error']=str(exc)
    return facts
