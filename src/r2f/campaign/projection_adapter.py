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


def _projection_roles(config):
    """Explicit roles opt into the generalized path; absence keeps the pair."""
    from r2f.runtime.projection_pair import projection_roles, projection_has_explicit_roles
    if not projection_has_explicit_roles(config):
        return ('writer', 'reader')
    return projection_roles(config)


def _lifecycle(events, runtime, config, observation):
    bindings = observation['endpoint_bindings']
    roles = _projection_roles(config)
    if len(bindings) != len(roles) or {b.get('role') for b in bindings} != set(roles):
        raise ValueError('projection bindings do not match projection_roles')
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
        # Per-role QoS overlays are submitted input too: a role declared in
        # ``projection_role_qos`` must be checked against that overlay, not
        # only against the base profile.
        expected_qos = _expected_role_qos(config, role)
        if any(e.get('qos') != expected_qos or e.get('topic') != config['topic'] or e.get('message_type') != config['message_type'] for e in (call, returned)):
            raise ValueError('actual create-call input differs from capsule')
        if not begin['monotonic_ns'] < call['monotonic_ns'] <= returned['monotonic_ns']:
            raise ValueError('projection endpoint creation preceded mutation begin')
        tokens[role] = {'id':guid, 'generation':binding['generation'], 'created_ns':returned['monotonic_ns'],
                        'pid':binding['pid'], 'process_start_ticks':binding['process_start_ticks']}
    if len({token['pid'] for token in tokens.values()}) != len(tokens):
        raise ValueError('projection lacks independent endpoint workers')
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


def _role_kind(role):
    from r2f.runtime.projection_pair import projection_role_kind
    return projection_role_kind(role)


def _expected_role_qos(config, role):
    """Base role QoS with the submitted per-role overlay applied.

    ``projection_role_qos`` is a first-class submitted input; ignoring it here
    made the inventory state an expectation the run never used.
    """
    overlay = (config.get('projection_role_qos') or {}).get(role) or {}
    return {**config.get(role + '_qos', config['qos']), **overlay}


def _graph_entries(graph, key, roles, tokens):
    """Validate graph rows for one side and return role -> list[entry]."""
    items = graph.get(key)
    if not isinstance(items, list):
        raise ValueError('projection graph ' + key + ' is not a list')
    by_gid = {token['id']: role for role, token in tokens.items()
              if (_role_kind(role) == 'writer') == (key == 'publishers')}
    observed, extras = {}, []
    for item in items:
        if not isinstance(item, dict) or not isinstance(item.get('endpoint_gid'), str):
            raise ValueError('projection graph endpoint entry incomplete')
        gid = item['endpoint_gid']
        if len(gid) not in (32, 48):
            raise ValueError('projection graph endpoint GUID malformed')
        prefix = gid[:32].lower()
        if prefix not in by_gid:
            extras.append(gid)
            continue
        role = by_gid[prefix]
        observed.setdefault(role, []).append(item)
    reasons = []
    for role, token in tokens.items():
        if (_role_kind(role) == 'writer') != (key == 'publishers'):
            continue
        rows = observed.get(role, [])
        if not rows:
            reasons.append('projection_graph_missing_endpoint:' + token['id'])
        elif len(rows) != 1:
            reasons.append('projection_graph_duplicate_endpoint:' + token['id'])
        else:
            item = rows[0]
            if item['endpoint_gid'][:32].lower() != token['id']:
                reasons.append('projection_graph_missing_endpoint:' + token['id'])
    for gid in extras:
        reasons.append('projection_graph_extra_endpoint:' + gid)
    return observed, sorted(set(reasons))


def content_endpoints(graph_snapshot):
    """Graph rows from the already hash-verified projection object."""
    rows = graph_snapshot.get('endpoints', [])
    return rows if isinstance(rows, list) else []


def _role_comparisons(discovery, graph, tokens, config):
    """Compare every configured role from the same independent wire and graph."""
    from r2f.instrumentation.sedp_fields import BASELINE_FIELDS
    from r2f.instrumentation.sedp_projection import _equal, _graph_fields
    graph_rows = {}
    for item in graph.get('endpoints', []):
        if not isinstance(item, dict) or not isinstance(item.get('endpoint_gid'), str):
            continue
        graph_rows[item['endpoint_gid'][:32].lower()] = item
    rows, unknown = [], []
    for role, token in tokens.items():
        latest = next((e for e in reversed(discovery.get('events', []))
                       if e.get('endpoint_guid') == token['id'] and e.get('lifecycle') == 'alive'), None)
        counterpart = graph_rows.get(token['id'])
        if latest is None:
            unknown.append('no_live_wire_endpoint_at_graph_snapshot:' + token['id'])
            continue
        if counterpart is None:
            # The graph was enumerated completely; representing the absence as
            # a structured observed value keeps a real projection gap a
            # mismatch instead of collapsing it into unknown evidence.
            absent = {'status': 'absent_from_graph',
                      'reason': 'projection_graph_missing_endpoint:' + token['id']}
            for name in BASELINE_FIELDS:
                observed = latest.get('fields', {}).get(name, {})
                rows.append({'endpoint_guid': token['id'], 'generation': token['generation'],
                             'role': role, 'field': name, 'equal': False,
                             'wire': deepcopy(observed), 'graph_value': deepcopy(absent),
                             'graph_raw': deepcopy(graph.get('raw'))})
            graph_rows[token['id']] = {'endpoint_gid': token['id'], 'role': role,
                                       'topic': latest.get('fields', {}).get('topic', {}).get('value'),
                                       'topic_type': None, 'qos': {}, 'absent_from_graph': True}
            continue
        fields = _graph_fields(counterpart)
        # Graph role is the explicit alias; wire only carries the base kind.
        fields['role'] = _role_kind(role)
        for name in BASELINE_FIELDS:
            observed = latest.get('fields', {}).get(name, {})
            value = _equal(name, observed.get('value'), fields.get(name)) if observed.get('status') == 'observed' else None
            row = {'endpoint_guid': token['id'], 'generation': token['generation'],
                   'role': role, 'field': name, 'equal': value,
                   'wire': deepcopy(observed), 'graph_value': fields.get(name),
                   'graph_raw': deepcopy(graph.get('raw'))}
            rows.append(row)
            if value is None:
                unknown.append('unobserved_field:' + token['id'] + ':' + name)
    return rows, unknown


def _primary_projection(rows, tokens, config):
    """Select a genuine writer/reader pair for the unchanged ProjectionFacts.

    When any role disagrees, the pair containing that disagreement is used, so
    the oracle sees a real unequal row instead of a synthesized verdict.
    """
    roles = tuple(tokens)
    writer_roles = [r for r in roles if _role_kind(r) == 'writer']
    reader_roles = [r for r in roles if _role_kind(r) == 'reader']
    unequal = {r['endpoint_guid'] for r in rows if r['equal'] is False}
    chosen = None
    for writers in writer_roles:
        for readers in reader_roles:
            pair = {writers, readers}
            pair_ids = {tokens[writers]['id'], tokens[readers]['id']}
            if pair_ids & unequal:
                chosen = (writers, readers)
                break
        if chosen:
            break
    if chosen is None:
        default_w = 'writer' if 'writer' in writer_roles else writer_roles[0]
        default_r = 'reader' if 'reader' in reader_roles else reader_roles[0]
        chosen = (default_w, default_r)
    primary = {tokens[r]['id'] for r in chosen}
    comparisons = [r for r in rows if r['endpoint_guid'] in primary]
    differences = [r for r in comparisons if r['equal'] is False]
    return chosen, comparisons, differences


def projection_endpoint_inventory(tokens, snapshots, config, wire_by_role):
    """Per-role observed facts: public graph identity, QoS and independent wire."""
    inventory = {}
    for role, token in tokens.items():
        writer = _role_kind(role) == 'writer'
        graph_rows = snapshots.get(role, {}).get('publishers' if writer else 'subscriptions', [])
        row = next((item for item in graph_rows
                    if isinstance(item, dict) and str(item.get('endpoint_gid', ''))[:32].lower() == token['id']),
                   None)
        wire = wire_by_role.get(role, {})
        wire_fields = wire.get('fields', {}) if isinstance(wire, dict) else {}
        expected_qos = _expected_role_qos(config, role)
        observed_qos = deepcopy(row.get('qos', {})) if row else None
        inventory[role] = {
            'role': role,
            'endpoint_kind': _role_kind(role),
            'gid': token['id'],
            'generation': token['generation'],
            'topic': row.get('topic', config['topic']) if row else config['topic'],
            'type': row.get('topic_type') if row else config['message_type'],
            'qos': observed_qos,
            'expected_qos': expected_qos,
            # Compare only the fields this endpoint contract declares.  The
            # public graph also reports at least ``history``, which is not a
            # submitted field here; including it would mark every honest
            # endpoint unequal and would turn a real observation into noise.
            'qos_equal': ({key: observed_qos.get(key) for key in expected_qos}
                          == expected_qos) if observed_qos is not None else None,
            'observed_in_graph': row is not None,
            'wire_seen': wire.get('lifecycle') == 'alive' if wire else False,
            'wire_topic': (wire_fields.get('topic') or {}).get('value'),
            'wire_type': (wire_fields.get('type') or {}).get('value'),
        }
    return inventory


def projection_graph_proof(events, config, observation, tokens):
    """Validate every role's graph chain; return counts, reasons and inventory."""
    exported = _one(events, 'projection_graph_exported')
    if exported.get('graph_snapshot') != observation['graph_snapshot']:
        raise ValueError('projection graph export differs from raw')
    proofs = exported.get('graph_receipts', [])
    roles = tuple(tokens)
    if len(proofs) != len(roles) or {p.get('role') for p in proofs} != set(roles):
        raise ValueError('projection graph requires one command chain per role')
    snapshots = {}
    commands = set()
    for proof in proofs:
        role = proof.get('role')
        token = tokens[role]
        def bound(key):
            # The runtime emits the long ``<key>_namespace_event_index`` form;
            # older fixtures use the short ``<key>_index``.  Both name the same
            # raw event, so accept either without inventing an index.
            for candidate in (key + '_namespace_event_index', key + '_index'):
                if candidate in proof:
                    return events[proof[candidate]]
            raise ValueError('projection graph receipt missing index:' + key)

        request, match, graph, ack = [bound(k)
                                      for k in ('request', 'match', 'snapshot', 'ack')]
        if [e.get('event') for e in (request, match, graph, ack)] != [
                'graph_snapshot_requested', 'match_snapshot', 'graph_snapshot', 'command_done']:
            raise ValueError('projection graph command chain event differs')
        command = proof['command_id']
        if type(command) is not int or command in commands:
            raise ValueError('projection command_id is not unique')
        commands.add(command)
        chain = [e for e in events if e.get('command_id') == command and e.get('role') == role
                 and e.get('event') in ('graph_snapshot_requested', 'match_snapshot',
                                        'graph_snapshot', 'command_done')]
        if chain != [request, match, graph, ack] or any(
                e.get('command_id') != command or e.get('role') != role for e in chain):
            raise ValueError('projection graph command identity is ambiguous')
        stamps = [request['requested_monotonic_ns'], match['sampled_monotonic_ns'],
                  match['monotonic_ns'], graph['monotonic_ns'], ack['monotonic_ns'],
                  exported['monotonic_ns']]
        if any(type(t) is not int for t in stamps) or stamps != sorted(stamps) or stamps[0] < token['created_ns']:
            raise ValueError('projection graph sample/ACK chronology invalid')
        for event in (match, graph, ack):
            if (event.get('pid') != token['pid']
                    or event.get('endpoint_generation', event.get('generation')) != token['generation']):
                raise ValueError('projection graph worker generation differs')
        expected_api = ('get_subscription_count' if _role_kind(role) == 'writer'
                        else 'get_publisher_count')
        if (match.get('count_api') != expected_api or type(match.get('matched_count')) is not int
                or match['matched_count'] < 0):
            raise ValueError('projection count diagnostic not a real public snapshot')
        snapshots[role] = graph
    reasons = []
    for role in roles:
        for key in ('publishers', 'subscriptions'):
            _, found = _graph_entries(snapshots[role], key, roles, tokens)
            reasons.extend(found)
    graph = observation['graph_snapshot']
    content = _json(graph['raw'])
    expected = {'source': 'ros_graph', 'captured_at_unix_ns': snapshots[roles[0]]['time_ns'], 'endpoints': []}
    for key in ('publishers', 'subscriptions'):
        for item in snapshots[roles[0]][key]:
            gid = item['endpoint_gid']
            owner = next((role for role, token in tokens.items()
                          if token['id'] == gid[:32].lower()), None)
            expected['endpoints'].append({'endpoint_gid': gid, 'role': owner or 'unknown',
                                          'topic': config['topic'], 'topic_type': item['topic_type'],
                                          'qos': deepcopy(item['qos'])})
    if content != expected or {k: v for k, v in graph.items() if k != 'raw'} != expected:
        raise ValueError('independent graph JSON not reproduced by public graph raw')
    begin = _one(events, 'mutation_begin' if config['arm'] == 'mutated' else 'control_begin')
    close = _one(events, 'window_close', window='endpoint_creation')
    window = observation['observation_window']
    if window != {'start_ns': begin['time_ns'], 'end_ns': exported['time_ns']} or not exported['monotonic_ns'] <= close['monotonic_ns']:
        raise ValueError('projection observation window differs from raw')
    offsets = [e['time_ns'] - e['monotonic_ns'] for e in (begin, exported, close)]
    if max(offsets) - min(offsets) > 50000000:
        raise ValueError('projection observation clock discontinuity')
    def proof_index(proof, key):
        for candidate in (key + '_namespace_event_index', key + '_index'):
            if candidate in proof:
                return proof[candidate]
        raise ValueError('projection graph receipt missing index:' + key)

    counts = {proof['role']: events[proof_index(proof, 'match')]['matched_count']
              for proof in proofs}
    return {'counts': counts, 'reasons': sorted(set(reasons)), 'snapshots': snapshots}


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
        roles = tuple(tokens)
        facts.update(tokens)
        # The canonical ProjectionFacts contract compares one independent
        # writer/reader pair. Select that pair from the observed facts after
        # proof validation, so the alias endpoints remain visible in inventory
        # without changing the established oracle schema.
        proof = projection_graph_proof(events,config,observation,tokens)
        counts = proof['counts']
        facts.update(endpoint_identity_complete=True)
        ready=_one(events,'discovery_observer_ready')
        stopped=_one(events,'discovery_observer_stopped')
        if observation['capture_receipt'].get('ready') != ready.get('ready') or stopped.get('receipt') != observation['capture_receipt']:
            raise ValueError('projection capture ready/stop receipt differs from raw')
        # This call always starts at independently captured packets; no graph or
        # submitted configuration is accepted as input to the wire parser.
        discovery = parse_sedp_capture(observation['capture_receipt'],field_policy=observation.get('field_policy'))
        # The canonical reconciler currently accepts writer/reader bindings.
        # Extra roles are compared below from the same independent wire/graph
        # sources, without narrowing the requested endpoint set.
        canonical = [b for b in observation['endpoint_bindings'] if b.get('role') in ('writer','reader')]
        kwargs = {'endpoint_bindings': canonical,
                  'observation_window': observation['observation_window'],
                  'field_policy': observation.get('field_policy')}
        graph_for_reconcile = deepcopy(observation['graph_snapshot'])
        original = reconcile_discovery_projection(discovery,graph_for_reconcile,**kwargs)
        result = original
        if roles != ('writer', 'reader'):
            rows, unknown = _role_comparisons(discovery, observation['graph_snapshot'], tokens, config)
            if unknown:
                raise ValueError('projection multi-role evidence incomplete:' + ','.join(sorted(set(unknown))))
            token_ids = {token['id'] for token in tokens.values()}
            graph_ids = {item['endpoint_gid'][:32].lower() for item in content_endpoints(observation['graph_snapshot'])}
            extras = sorted(graph_ids - token_ids)
            graph_reasons = list(proof['reasons'])
            graph_reasons.extend('projection_graph_extra_endpoint:' + gid for gid in extras)
            pair, comparisons, differences = _primary_projection(rows, tokens, config)
            if extras:
                # An unexpected publisher/subscription is an identity-set
                # mismatch for the primary endpoint: the finite graph did not
                # identify exactly the submitted endpoints. Record the actual
                # observed set, never a synthesized field value.
                anchor = next((r for r in comparisons
                               if r['field'] == 'endpoint_guid'
                               and r['role'] == pair[0]), comparisons[0])
                anchor['graph_value'] = {'status': 'observed_endpoint_set',
                                         'expected_guid': anchor['endpoint_guid'],
                                         'unexpected_guids': sorted(extras)}
                anchor['equal'] = False
                differences = [r for r in comparisons if r['equal'] is False]
            result = dict(original)
            result.update(comparison_status='mismatch' if differences else 'consistent',
                          comparisons=comparisons, differences=differences,
                          unknown_reasons=sorted(set(result.get('unknown_reasons', []))),
                          projection_graph_reasons=sorted(set(graph_reasons)),
                          primary_pair=pair,
                          endpoint_count=len(roles))
            original = dict(original)
            original.update(projection_graph_reasons=sorted(set(graph_reasons)),
                            primary_pair=pair, endpoint_count=len(roles))
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
        healthy = (len(workers)==len(roles) and {w.get('role') for w in workers}==set(roles)
                   and all(w.get('returncode')==0 and w.get('pid')==tokens[w['role']]['pid'] for w in workers)
                   and _one(events,'namespace_controller_exited').get('returncode')==0
                   and runtime.get('runtime_status')=='completed'
                   and runtime.get('carrier_implementation')=='isolated_netns_passive_sedp_public_qos'
                   and not any(e.get('event') in ('worker_error','worker_unexpected_exit','runtime_abort','owned_worker_cleanup_signal') for e in events))
        window = observation['observation_window']
        observed_endpoints={role:next((e for e in reversed(discovery['events']) if e.get('endpoint_guid')==token['id'] and e.get('lifecycle')=='alive'),{}) for role,token in tokens.items()}
        compatibility=check_offered_requested(observed_endpoints['writer'],observed_endpoints['reader'])
        inventory = projection_endpoint_inventory(tokens, proof['snapshots'], config, observed_endpoints)
        primary_roles = result.get('primary_pair') or ('writer', 'reader')
        alias_writer = next((r for r in primary_roles if _role_kind(r) == 'writer'), None)
        alias_reader = next((r for r in primary_roles if _role_kind(r) != 'writer'), None)
        for role in roles:
            facts.pop(role, None)
        # The established ProjectionFacts parser requires keys named exactly
        # writer/reader. Preserve each pair member's observed facts under that
        # canonical pair key; the full alias map remains in the inventory.
        if alias_writer and alias_reader:
            facts['writer'] = deepcopy(tokens[alias_writer])
            facts['reader'] = deepcopy(tokens[alias_reader])
        facts.update(projection=result, original_projection=original,
                     projection_endpoint_inventory=inventory,
                     projection_graph_reasons=proof['reasons'],
                     window={'start_ns':window['start_ns'],'end_ns':window['end_ns'],'complete':not result['unknown_reasons']},
                     process_healthy=healthy,preconditions_reached=True,
                     observed_effects=['expected_qos_projection'] if not original['unknown_reasons'] else [],
                     diagnostics={'actual_match':{'counts':counts,'observed':all(c==len(roles)-1 for c in counts.values()),
                                                  'role_count':len(roles),'finite_no_match':'unknown'},
                                  'offered_requested':compatibility,
                                  'projection_graph_reasons':proof['reasons'],
                                  'projection_endpoint_count':len(roles)})
        if proof['reasons']:
            # Missing/extra endpoints are an observed S1 projection mismatch.
            # Keep them visible even when the canonical pair fields agree.
            facts['projection_graph_inconsistent'] = True
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
    from .state_projection import attach_state_observations
    return attach_state_observations(facts, events, runtime, config)
