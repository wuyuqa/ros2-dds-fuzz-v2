"""Read-only qualification of bridge sidecars before existing lifecycle intake.

An empty intake and a completed, provenance-verified zero observation are distinct.
No sidecar can replace canonical execution evidence or authorize a blackbox run.
"""
from copy import deepcopy
import hashlib
import json
import xml.etree.ElementTree as ET
from pathlib import Path
from r2f.core._json import sha256_digest
from r2f.core.evidence import canonical_hash, verify_raw
from r2f.core.replay import ReplayCapsule, validate_replay_input
from r2f.instrumentation.cyclonedds import TraceConfig, collect_trace, normalize_guid
from r2f.instrumentation.projection import reconcile_projection
from .discovery_profile import discovery_profile_facts
from r2f.runtime.instrumentation_bridge import (
    FASTDDS_RMW, FASTDDS_DEFAULT_XML, observation_policy, fastdds_environment_key,
    fastdds_libraries_verified, mapped_library_identity_matches,
)

CONTRACT = 'cyclonedds.received_sedp.identity_role_reliability_durability/v1'
_INPUT_IDENTITY = ('target_id', 'target_revision', 'rmw', 'arm', 'repeat')
_EXECUTION_IDENTITY = ('run_id', *_INPUT_IDENTITY, 'domain_id')
_OBSERVATION_INPUT = (*_EXECUTION_IDENTITY, 'seed', 'topic', 'message_type',
                      'effective_input_sha256', 'final_input', 'input_layer_receipts')


def _read(ref):
    verify_raw(ref['raw_path'], ref['raw_sha256'])
    return json.loads(Path(ref['raw_path']).read_text())


def _bind_input(row, runtime, config, observation, execution, capsule):
    """Bind the capsule-derived canonical row to the actual execution and sidecar.

    ingest_execution supplies the canonical hash from its validated capsule. An
    explicit capsule (or a persisted row's replay_path) additionally revalidates
    the complete input here; prepared and summary bind the actual execution_input.
    A hash present anywhere is never ignored, including in either metadata alias.
    """
    digest = sha256_digest(row.get('effective_input_sha256'), 'canonical effective_input_sha256')
    if config.get('effective_input_sha256') != digest:
        raise ValueError('config effective input hash differs from canonical capsule hash')
    for key in _EXECUTION_IDENTITY:
        if key not in config or config[key] is None:
            raise ValueError('runtime config identity missing:' + key)
        if key not in row or row[key] != config[key]:
            raise ValueError('canonical execution identity differs:' + key)
        if execution.get(key) != config[key]:
            raise ValueError('runtime execution identity differs:' + key)

    # The runtime's execution / execution_metadata objects are aliases of the
    # same required identity record. That contract has execution identity and
    # raw refs, but no effective-input hash; the hash is independently bound by
    # capsule -> config -> canonical row -> sidecar execution_input below.
    execution_alias = runtime.get('execution')
    execution_metadata = runtime.get('execution_metadata')
    if execution_alias is None and execution_metadata is None:
        raise ValueError('runtime execution metadata missing')
    if execution_alias is not None and execution_metadata is not None:
        if canonical_hash(execution_alias) != canonical_hash(execution_metadata):
            raise ValueError('runtime execution aliases differ')

    # Top-level runtime identity fields are not part of the run_pair result
    # contract. When supplied, they must agree; its raw refs are mandatory and
    # bind the actual raw to the required execution metadata identity above.
    records = [('runtime', runtime), ('execution', execution_alias),
               ('execution_metadata', execution_metadata), ('observation', observation)]
    for name, record in records:
        if record is None:
            continue
        for key in _EXECUTION_IDENTITY:
            if name in ('execution', 'execution_metadata') and key not in record:
                raise ValueError(name + ' identity missing:' + key)
            if key in record and record[key] != config[key]:
                raise ValueError(name + ' input/execution identity differs:' + key)
        # Runtime and sidecar schemas do not carry this field at their top
        # levels. Any occurrence is still checked; capsule/config/execution_input
        # form the required effective-input binding chain.
        if 'effective_input_sha256' in record and record['effective_input_sha256'] != digest:
            raise ValueError(name + ' input/execution identity differs:effective_input_sha256')
        for key in ('raw_path', 'raw_sha256'):
            if record.get(key) != row.get(key):
                raise ValueError(name + ' raw identity differs:' + key)
    final_input = config.get('final_input')
    if not isinstance(final_input, dict) or not final_input:
        raise ValueError('final submitted input missing')
    for key, value in final_input.items():
        if key not in config or config[key] != value:
            raise ValueError('submitted input differs from runtime config:' + key)
    capsules = []
    if capsule is not None:
        capsules.append(capsule.input if isinstance(capsule, ReplayCapsule) else capsule['input'] if 'input' in capsule else capsule)
    if row.get('replay_path'):
        capsules.append(json.loads(Path(row['replay_path']).read_text())['input'])
    if not capsules:
        raise ValueError('validated replay capsule required for observation qualification')
    for data in capsules:
        validate_replay_input(data)
        if data['effective_input_sha256'] != digest:
            raise ValueError('capsule effective input hash differs')
        for key in (*_INPUT_IDENTITY, 'seed', 'topic', 'message_type'):
            if (canonical_hash(data[key]) != canonical_hash(config.get(key))
                    or (key in row and canonical_hash(data[key]) != canonical_hash(row[key]))
                    or (key == 'seed' and key not in row)):
                raise ValueError('capsule input identity differs:' + key)
        if data['source_manifest'] != row.get('source_manifest'):
            raise ValueError('capsule source differs from canonical source')
        if data['effective_input']['final_input'] != final_input:
            raise ValueError('capsule final input differs from runtime config')
        if 'scenario' in config and config['scenario'] != data['scenario']:
            raise ValueError('capsule scenario differs from runtime config')
        if 'input_layer_receipts' in config and config['input_layer_receipts'] != data['effective_input']['input_layer_receipts']:
            raise ValueError('capsule input layer receipts differ')
    if 'source_manifest' in config and config['source_manifest'] != row.get('source_manifest'):
        raise ValueError('config source differs from canonical capsule source')
    submitted = observation.get('execution_input')
    if not isinstance(submitted, dict) or submitted.get('schema') != 'r2f.observation-execution-input/v1':
        raise ValueError('observation execution_input missing or unsupported')
    for key in _OBSERVATION_INPUT:
        if key not in submitted or key not in config or canonical_hash(submitted[key]) != canonical_hash(config[key]):
            raise ValueError('observation execution_input differs:' + key)
        if key in row and canonical_hash(submitted[key]) != canonical_hash(row[key]):
            raise ValueError('observation execution_input differs from canonical row:' + key)
        for name, record in (('runtime', runtime), ('execution', execution_alias),
                             ('execution_metadata', execution_metadata)):
            if record is not None and key in record and canonical_hash(record[key]) != canonical_hash(submitted[key]):
                raise ValueError(name + ' execution_input differs:' + key)
    return {'effective_input_sha256': digest,
            'execution_input_sha256': canonical_hash(submitted),
            'execution_identity': {k: config[k] for k in _EXECUTION_IDENTITY},
            'capsule_validation': 'verified',
            'runtime_binding': 'execution_metadata_identity_plus_verified_raw_sha256',
            'sidecar_binding': 'execution_input_plus_prepared_and_verified_raw_sha256'}


def _profile_node(element):
    return [element.tag, sorted(element.attrib.items()), (element.text or '').strip(),
            [_profile_node(child) for child in element]]


def _profile_hash(root):
    return hashlib.sha256(json.dumps(_profile_node(root), sort_keys=True).encode()).hexdigest()


def _semantic_profiles(qualification, domain):
    """Normalize formatting and this execution's Domain binding, nothing else."""
    if type(domain) is not int or not 0 <= domain <= 231:
        raise ValueError('transport profile execution domain missing or invalid')
    profiles = qualification.get('non_tracing_profiles')
    if not isinstance(profiles, dict) or set(profiles) != {'writer', 'reader'}:
        raise ValueError('two non-Tracing transport profiles required')
    values = []
    for role, profile in sorted(profiles.items()):
        root = ET.fromstring(profile['xml'])
        tag = lambda e: e.tag.rsplit('}', 1)[-1]
        if tag(root) != 'CycloneDDS' or any(tag(e) == 'Tracing' for e in root.iter()):
            raise ValueError('transport profile invalid or enables Tracing')
        if profile.get('non_tracing_sha256') != _profile_hash(root):
            raise ValueError('transport profile semantic hash differs')
        domains = [e for e in root if tag(e) == 'Domain']
        if len(domains) != 1:
            raise ValueError('one transport profile Domain required')
        section = domains[0]
        bindings = [section.attrib[k] for k in ('Id', 'id') if k in section.attrib]
        if len(bindings) > 1 or (bindings and bindings[0] not in ('any', str(domain))):
            raise ValueError('transport profile bound to another execution domain')
        for key in ('Id', 'id'):
            section.attrib.pop(key, None)
        values.append([role, _profile_node(root)])
    return values


def _audit_adopted_settings(observation, prepared, workers, rebuilt, config):
    """Rebuild execution metadata from bound artifacts, never from its summary."""
    expected = {'schema': 'r2f.adopted-domain-settings/v1', 'roles': {},
                'status': 'not_run', 'semantic_effect': 'unknown',
                'parser_acceptance': 'pending_real_trace',
                'fastdds': {'status': 'unsupported', 'execution': 'not_run'}}
    if config['rmw'] == FASTDDS_RMW:
        expected['status'] = 'unsupported'
    elif config.get('observe_adopted_config', False):
        for role, trace_config in prepared['trace_configs'].items():
            matches = [t for t in rebuilt if canonical_hash(t['config']) == canonical_hash(trace_config)]
            if len(matches) != 1:
                raise ValueError('adopted trace role not unique:' + role)
            adopted = deepcopy(matches[0]['adopted_domain_settings'])
            worker = workers[role]
            endpoint = worker['endpoint_event']
            if (not endpoint.get('endpoint_generation')
                    or str(endpoint.get('process_start_ticks', '')) != str(worker['process']['process_start_ticks'])):
                raise ValueError('adopted process lifetime not bound to raw:' + role)
            # collect_trace recomputes this identity from the actual trace stat;
            # the process proof and the saved FD audit must both bind that file.
            if worker['audit'].get('trace_file_identity') != adopted['collection_identity'][:2]:
                raise ValueError('trace file identity differs from process audit:' + role)
            adopted['endpoint_binding'] = {
                'role': role, 'pid': worker['pid'], 'endpoint_guid': worker['endpoint_guid'],
                'endpoint_generation': worker['endpoint_event']['endpoint_generation'],
                'endpoint_event': deepcopy(worker['endpoint_event']),
                'process_capture': deepcopy(observation['process_captures'][role]),
                'runtime_raw_path': observation['raw_path'],
                'runtime_raw_sha256': observation['raw_sha256']}
            adopted['binding_unknowns'] = []
            adopted['status'] = 'unknown' if adopted['unknown_fields'] else 'observed'
            expected['roles'][role] = adopted
        expected['status'] = ('observed' if len(expected['roles']) == 2 and
                              all(r['status'] == 'observed' for r in expected['roles'].values())
                              else 'unknown')
    presented = {role: {'scope': 'parent_prepared',
                         'worker_byte_read': 'not_observed',
                         'input_xml_source': deepcopy(profile['source']),
                         'non_tracing_sha256': profile['non_tracing_sha256'],
                         'trace_xml_sha256': (TraceConfig(**prepared['trace_configs'][role]).sha256
                                              if role in prepared.get('trace_configs', {}) else None),
                         'prepared': deepcopy(observation['prepared'])}
                for role, profile in prepared.get('profiles', {}).items()}
    if observation.get('semantic_effect') != 'unknown':
        raise ValueError('adoption semantic_effect must remain unknown')
    if canonical_hash(observation.get('presented_configuration')) != canonical_hash(presented):
        raise ValueError('presented_configuration differs from prepared configuration')
    if canonical_hash(observation.get('adopted_domain_settings')) != canonical_hash(expected):
        raise ValueError('adopted_domain_settings differs from independent recomputation')
    return expected


def qualify_observation(row, runtime, config, *, capsule=None):
    observation = runtime.get('observation')
    result = {'schema':'r2f.campaign-observation-intake/v1', 'observation_status':'not_run',
              'qualification':'not_evaluated', 'intake_count':None, 'admissible_count':None,
              'qualified_intake':[], 'errors':[], 'blackbox_task':None, 'zero_candidate':False,
              'configuration_presentation_status':'legacy_unverified',
              'configuration_adoption_status':'unknown',
              'adoption_audit_status':'legacy_unverified', 'semantic_effect':'unknown'}
    if observation is None:
        return result
    result.update(observation_status=observation.get('status','unknown'),
                  intake_count=len(observation['intake']) if isinstance(observation.get('intake'),list) else None,
                  observation_mode=observation.get('observation_mode'), artifact=observation.get('artifact'),
                  blackbox_task=deepcopy(observation.get('blackbox_task')))
    try:
        policy = observation_policy(config.get('rmw'), config.get('observation_mode', 'blackbox'))
        stored = _read(observation['artifact'])
        if canonical_hash(stored) != canonical_hash({k:v for k,v in observation.items() if k!='artifact'}):
            raise ValueError('observation summary differs from hash-bound artifact')
        if observation.get('schema') != 'r2f.runtime-instrumentation/v1':
            raise ValueError('unsupported observation contract')
        if (observation.get('status')!='completed' or observation.get('verdict') not in ('pass','fail')
                or observation.get('unknown_fields') or observation.get('actual_run') is not True):
            raise ValueError('observation incomplete or unknown')
        for key in ('run_id','raw_path','raw_sha256'):
            if observation.get(key) != row.get(key) or observation.get(key) != runtime.get(key,config.get(key)):
                raise ValueError('observation execution identity differs:'+key)
        execution = runtime.get('execution_metadata') or runtime.get('execution') or {}
        input_binding = _bind_input(row, runtime, config, observation, execution, capsule)
        if observation.get('source_manifest') != execution.get('source'):
            raise ValueError('observation runtime source differs')
        source = row.get('source_manifest') or {}
        if any(source.get(k) is not None and source[k] != execution.get('source',{}).get(k) for k in ('head','source_repo')):
            raise ValueError('observation canonical source differs')
        if not source.get('head') or any(execution.get('source',{}).get(k) != execution.get('source_after',{}).get(k) for k in ('head','runtime_source_sha256','observer_source_sha256')):
            raise ValueError('observation source changed during execution')
        if row.get('actual_run') is not True or row.get('execution_stage')!='judged':
            raise ValueError('canonical execution not judged')
        verify_raw(row['raw_path'], row['raw_sha256'])
        events = [json.loads(line) for line in Path(row['raw_path']).read_text().splitlines() if line.strip()]
        prepared = _read(observation['prepared'])
        if config.get('observe_adopted_config') is True and 'adopted_domain_settings' not in observation:
            raise ValueError('adopted_domain_settings missing for explicit observe_adopted_config')
        for key in ('run_id','domain_id','rmw'):
            if prepared.get(key) != config.get(key):
                raise ValueError('prepared identity differs:'+key)
        if observation.get('observation_mode') not in ('instrumentation', 'blackbox'):
            raise ValueError('unsupported observation mode')
        if prepared.get('observation_mode') != observation.get('observation_mode') or observation.get('observation_mode') != config.get('observation_mode', 'blackbox'):
            raise ValueError('observation execution mode differs')
        if canonical_hash(prepared.get('execution_input')) != canonical_hash(observation.get('execution_input')):
            raise ValueError('prepared execution_input differs from observation')
        if observation.get('profile_policy') != policy or prepared.get('profile_policy') != policy:
            raise ValueError('observation profile applicability differs')
        fastdds = config['rmw'] == FASTDDS_RMW
        if fastdds:
            if prepared.get('profiles') != {} or prepared.get('trace_configs') != {}:
                raise ValueError('FastDDS Cyclone profiles and trace configs must be explicitly empty')
            if observation.get('traces') != [] or observation.get('reconciliations') != []:
                raise ValueError('FastDDS blackbox cannot carry Cyclone traces or projections')
            profiles = None
        else:
            profiles = _semantic_profiles({'non_tracing_profiles': prepared.get('profiles')}, config['domain_id'])
        if prepared.get('observer_source_sha256') != observation.get('observer_source_sha256') or not observation.get('observer_source_sha256'):
            raise ValueError('observer source provenance missing')
        workers, processes = {}, {}
        captures = observation.get('process_captures',{})
        if set(captures) != {'writer','reader'}:
            raise ValueError('two process captures required')
        metadata_workers = execution.get('workers',[])
        if isinstance(metadata_workers,dict):metadata_workers=list(metadata_workers.values())
        for role, ref in captures.items():
            worker = _read(ref); proof=worker['process']; token=worker['endpoint_event']
            if worker.get('accepted') is not True or worker.get('audit',{}).get('errors') or proof.get('unknown') or proof.get('process_identity_stable') is not True:
                raise ValueError('process provenance incomplete:'+role)
            if token not in events or token.get('event')!='endpoint_created' or token.get('role')!=role:
                raise ValueError('worker endpoint not bound to raw:'+role)
            if worker.get('role') != role:
                raise ValueError('worker capture role differs from raw:' + role)
            if proof.get('pid') != worker['pid']:
                raise ValueError('worker process PID differs from raw:' + role)
            gid = token.get('endpoint_gid')
            if isinstance(gid, str) and len(gid) == 48:
                gid = bytes.fromhex(gid)
            if worker.get('endpoint_guid') != normalize_guid(gid):
                raise ValueError('worker endpoint GUID differs from raw:' + role)
            for key in ('pid','run_id','domain_id','rmw_identifier'):
                expected = worker['pid'] if key=='pid' else config['rmw'] if key=='rmw_identifier' else config.get(key)
                if token.get(key)!=expected:
                    raise ValueError('worker raw identity differs:'+role+':'+key)
            if not any(w.get('role')==role and w.get('pid')==worker['pid'] and w.get('pgid')==proof.get('process_group') and w.get('returncode')==0 for w in metadata_workers):
                raise ValueError('worker process exit provenance absent:'+role)
            for lib in proof.get('loaded_libraries',[]):
                if lib.get('identity_verified') is not True:
                    raise ValueError('loaded binary provenance unknown')
                verify_raw(lib['path'],lib['sha256'])
            workers[role]=worker;processes[role]=proof
        if workers['writer']['pid']==workers['reader']['pid']:
            raise ValueError('observer workers not independent')
        # Recheck saved live-process audits from their hash-bound captures.
        roots=[Path(p).resolve() for p in config.get('instrumentation_roots',())]
        def under(path):
            resolved=Path(path).resolve()
            return any(resolved==r or r in resolved.parents for r in roots)
        process_profiles = {}
        for role,worker in workers.items():
            proof=worker['process'];audit=worker['audit'];env=audit.get('environment',{})
            if any(k in ('LD_PRELOAD','LD_AUDIT') or k.startswith(('R2F_INSTRUMENT','CYCLONEDDS_TRACE')) for k in env):
                raise ValueError('process audit contains instrumentation loader override')
            if env.get('ROS_DOMAIN_ID')!=str(config['domain_id']) or env.get('RMW_IMPLEMENTATION')!=config['rmw']:
                raise ValueError('process audit environment identity differs')
            for key in ('PATH','LD_LIBRARY_PATH','PYTHONPATH','AMENT_PREFIX_PATH','CMAKE_PREFIX_PATH','COLCON_PREFIX_PATH'):
                if any(under(p) for p in env.get(key,'').split(':') if p):
                    raise ValueError('instrumentation root appears in search path')
            if not proof.get('process_start_ticks') or str(proof['process_start_ticks'])!=str(worker['endpoint_event'].get('process_start_ticks',proof['process_start_ticks'])):
                raise ValueError('process audit generation differs')
            for library in proof.get('loaded_libraries',[]):
                if (under(library['path'])
                        or not mapped_library_identity_matches(library, audit.get('maps', ''))):
                    raise ValueError('loaded binary differs from saved process maps')
            if fastdds:
                if any(fastdds_environment_key(k) for k in env):
                    raise ValueError('unsupported FastDDS profile or trace environment')
                if not fastdds_libraries_verified(proof.get('loaded_libraries',[])) or proof.get('vendor_loaded') is not True:
                    raise ValueError('FastDDS loaded binary provenance incomplete')
                defaults = audit.get('default_profile_files')
                cwd = audit.get('cwd')
                if (not isinstance(cwd,str) or not Path(cwd).is_absolute() or not isinstance(defaults,dict)
                        or set(defaults) != set(FASTDDS_DEFAULT_XML)
                        or any(defaults[name] != {'path': str(Path(cwd)/name), 'exists': False} for name in FASTDDS_DEFAULT_XML)):
                    raise ValueError('FastDDS default profile absence unverified')
            if observation.get('observation_mode')=='blackbox':
                xml=env.get('CYCLONEDDS_URI')
                if xml and any(e.tag.rsplit('}',1)[-1]=='Tracing' for e in ET.fromstring(xml).iter()):
                    raise ValueError('blackbox process enables tracing')
                directory = Path(observation['prepared']['raw_path']).resolve().parent
                if proof.get('trace_fd_attached') or any(under(p) or Path(p).resolve().is_relative_to(directory) for p in audit.get('fd_targets',[]) if p.startswith('/')):
                    raise ValueError('blackbox process attached instrumentation fd')
            if fastdds:
                continue
            root = ET.fromstring(env.get('CYCLONEDDS_URI') or
                                 '<CycloneDDS><Domain Id="' + str(config['domain_id']) + '" /></CycloneDDS>')
            if observation.get('observation_mode') == 'instrumentation':
                for parent in (root, *list(root)):
                    for child in list(parent):
                        if child.tag.rsplit('}', 1)[-1] == 'Tracing':
                            parent.remove(child)
            process_profiles[role] = {'xml': ET.tostring(root, encoding='unicode'),
                                      'non_tracing_sha256': _profile_hash(root)}
        if not fastdds and _semantic_profiles({'non_tracing_profiles': process_profiles}, config['domain_id']) != profiles:
            raise ValueError('prepared transport profiles differ from process environment')
        task=observation.get('blackbox_task')
        if observation.get('observation_mode')=='blackbox':
            if task is not None:
                raise ValueError('blackbox execution must not carry a planned task')
            # A verified clean blackbox observation is linkage evidence, never an
            # instrumented zero or automatic confirmation of another execution.
            if observation.get('blackbox_observer',{}).get('status')!='verified':
                raise ValueError('blackbox clean observer not verified')
            if 'adopted_domain_settings' in observation:
                audited = _audit_adopted_settings(observation, prepared, workers, [], config)
                result.update(configuration_presentation_status='verified',
                              configuration_adoption_status=audited['status'],
                              adoption_audit_status='verified', semantic_effect='unknown',
                              adopted_domain_settings=deepcopy(observation['adopted_domain_settings']))
            elif 'presented_configuration' in observation:
                raise ValueError('presented_configuration present without adopted-domain audit contract')
            result['discovery_profile'] = discovery_profile_facts(
                config, prepared, workers, [], [], observation.get('adopted_domain_settings', {}))
            result.update(qualification='blackbox_observed', provenance_verified=True, input_binding=input_binding,
                          non_tracing_profiles=deepcopy(prepared['profiles']), profile_policy=deepcopy(policy))
            return result
        if not isinstance(task,dict) or task.get('status')!='not_run' or task.get('executed_by_bridge') is not False or task.get('origin_run_id')!=config['run_id']:
            raise ValueError('same-input blackbox task lacks not-run provenance')
        same_input=task.get('same_input')
        common_input = (*_INPUT_IDENTITY, 'seed', 'topic', 'message_type', 'final_input',
                        'effective_input_sha256', 'input_layer_receipts')
        if not isinstance(same_input,dict) or any(k not in same_input or canonical_hash(same_input[k])!=canonical_hash(config.get(k)) for k in common_input):
            raise ValueError('blackbox task effective input differs')
        if _semantic_profiles(task, config['domain_id']) != profiles:
            raise ValueError('blackbox task transport profiles differ')
        traces=observation.get('traces',[])
        if len(traces)!=2:
            raise ValueError('two independent closed traces required')
        rebuilt=[]
        for trace in traces:
            roles=[r for r,c in prepared['trace_configs'].items() if canonical_hash(c)==canonical_hash(trace.get('config'))]
            if len(roles)!=1:
                raise ValueError('trace config not bound to prepared artifact')
            role=roles[0]
            actual=collect_trace(TraceConfig(**trace['config']),processes=[processes[role]],control_task=task)
            for key in ('raw_path','raw_sha256','events','config','processes','vendor_process_verified','internal_event_witness'):
                if actual.get(key)!=trace.get(key):
                    raise ValueError('trace recomputation differs:'+key)
            if not actual.get('vendor_process_verified') or not actual.get('internal_event_witness'):
                raise ValueError('empty or unverified internal observation')
            rebuilt.append(actual)
        if 'adopted_domain_settings' in observation:
            audited = _audit_adopted_settings(observation, prepared, workers, rebuilt, config)
            result.update(configuration_presentation_status='verified',
                          configuration_adoption_status=audited['status'],
                          adoption_audit_status='verified', semantic_effect='unknown',
                          adopted_domain_settings=deepcopy(observation['adopted_domain_settings']))
        elif 'presented_configuration' in observation:
            raise ValueError('presented_configuration present without adopted-domain audit contract')
        comparisons=observation.get('reconciliations',[])
        if len(comparisons)!=2 or {c.get('role') for c in comparisons}!={'writer','reader'}:
            raise ValueError('two independent graph comparisons required')
        for comparison in comparisons:
            role= comparison['role']; graph=comparison['graph']; content=_read(graph)
            if content!={k:v for k,v in graph.items() if k not in ('raw_path','raw_sha256')}:
                raise ValueError('graph projection differs from artifact')
            token=content.get('application_event')
            if token not in events or token.get('role')!=role or token.get('pid')!=workers[role]['pid']:
                raise ValueError('graph source not bound to worker raw')
            chain=[e for e in events if e.get('role')==role and e.get('command_id')==token.get('command_id') and e.get('event') in ('graph_snapshot_requested','match_snapshot','graph_snapshot','command_done')]
            if len(chain)!=4 or [e['event'] for e in chain]!=['graph_snapshot_requested','match_snapshot','graph_snapshot','command_done'] or chain[2]!=token:
                raise ValueError('graph command request/snapshot/ACK binding differs')
            times=[chain[0]['requested_monotonic_ns'],chain[1]['sampled_monotonic_ns'],chain[1]['monotonic_ns'],token['monotonic_ns'],chain[3]['monotonic_ns']]
            if any(type(t)is not int for t in times) or times!=sorted(times) or chain[1].get('matched_count')!=1:
                raise ValueError('graph command timing or match proof incomplete')
            actual=reconcile_projection(rebuilt,graph,dds_topic='rt'+config['topic'],ros_topic=config['topic'],
                                        complete_window=True,window_start_ns=prepared['started_unix_ns'],window_end_ns=graph['captured_at_unix_ns'])
            if actual!=comparison['comparison'] or actual.get('unknown_fields'):
                raise ValueError('limited contract recomputation incomplete or differs')
        witnesses=[]
        expected_guids={normalize_guid(bytes.fromhex(w['endpoint_event']['endpoint_gid']) if len(w['endpoint_event']['endpoint_gid'])==48 else w['endpoint_event']['endpoint_gid']) for w in workers.values()}
        for trace in rebuilt:
            for event in trace['events']:
                if event.get('topic_name')=='rt'+config['topic'] and (event.get('domain_id')!=config['domain_id'] or event.get('endpoint_guid') not in expected_guids):
                    raise ValueError('internal event not bound to this endpoint pair')
        for entry in comparisons:
            graph=entry['graph']
            for mismatch in entry['comparison']['mismatches']:
                matching=[e for t in rebuilt for e in t['events'] if e.get('endpoint_guid')==mismatch['endpoint_guid']
                          and e.get('topic_name')=='rt'+config['topic'] and prepared['started_unix_ns']<=e['event_time_ns']<=graph['captured_at_unix_ns']]
                if not matching:raise ValueError('limited failure has no internal witness')
                witnesses.append({'comparison_role':entry['role'],'field':mismatch['field'],
                                  'graph_raw_path':graph['raw_path'],'graph_raw_sha256':graph['raw_sha256'],
                                  'vendor_event':max(matching,key=lambda e:e['event_time_ns'])})
        verdict='fail' if any(c['comparison']['verdict']=='fail' for c in comparisons) else 'pass'
        if observation['verdict']!=verdict:
            raise ValueError('summary verdict differs from limited contract')
        intake=observation.get('intake',[])
        if type(observation.get('candidate_count')) is not int or observation['candidate_count']!=len(intake):
            raise ValueError('intake count differs from records')
        excluded=config.get('arm')=='negative' or bool(config.get('control_kind'))
        if excluded and intake:
            raise ValueError('control entered instrumentation intake')
        expected_count=1 if verdict=='fail' and not excluded else 0
        if len(intake)!=expected_count:
            raise ValueError('limited anomaly dropped or invented')
        for item in intake:
            for key in ('run_id','target_id','target_revision','rmw','arm','repeat','effective_input_sha256'):
                if item.get(key)!=config.get(key):raise ValueError('intake identity differs:'+key)
            if item.get('internal_contract')!=CONTRACT or item.get('raw_path')!=row['raw_path'] or item.get('raw_sha256')!=row['raw_sha256']:
                raise ValueError('intake contract/raw differs')
            if item.get('execution_metadata')!=execution:
                raise ValueError('intake execution provenance differs')
            if item.get('observed_effect') != [e['comparison'] for e in comparisons if e['comparison']['verdict']=='fail']:
                raise ValueError('intake effect not bound to independently recomputed contract')
            expected_identity={'id':CONTRACT,'raw_hashes':[t['raw_sha256'] for t in traces]}
            if item.get('instrumentation_identity') != expected_identity:
                raise ValueError('intake instrumentation identity differs')
            provenance=item.get('provenance',{})
            if not all(provenance.get(k) for k in ('instrumentation_point','event_time','control','binary_identity','witnesses')):
                raise ValueError('instrumentation witness provenance incomplete')
            expected_time=max(w['vendor_event']['event_time_ns'] for w in witnesses)
            expected_points=','.join(sorted({w['vendor_event']['instrumentation_point'] for w in witnesses}))
            if (provenance['witnesses']!=witnesses or provenance['event_time']!=str(expected_time)
                    or provenance['instrumentation_point']!=expected_points
                    or provenance['binary_identity']!=[workers[r]['process']['loaded_libraries'] for r in ('writer','reader')]
                    or provenance.get('process_captures')!=observation['process_captures']):
                raise ValueError('intake provenance differs from actual internal witnesses')
            for witness in provenance['witnesses']:
                event=witness['vendor_event']
                if not any(event in t['events'] for t in rebuilt):
                    raise ValueError('instrumentation witness not bound to trace')
        result['discovery_profile'] = discovery_profile_facts(
            config, prepared, workers, rebuilt, comparisons, observation.get('adopted_domain_settings', {}))
        result.update(qualification='qualified',provenance_verified=True,limited_contract=CONTRACT,
                      admissible_count=len(intake),qualified_intake=deepcopy(intake),zero_candidate=not intake and not excluded,
                      observation_verdict=verdict, input_binding=input_binding, non_tracing_profiles=deepcopy(prepared['profiles']), profile_policy=deepcopy(policy))
    except (KeyError,ValueError,TypeError,OSError,IndexError,AttributeError,ET.ParseError) as exc:
        result['errors'].append(str(exc))
    return result


def instrumentation_evidence(row, item):
    """Enrich an intake with its validated canonical row; core owns admission."""
    sha256_digest(row.get('effective_input_sha256'), 'canonical effective_input_sha256')
    for key in ('run_id', *_INPUT_IDENTITY, 'effective_input_sha256', 'raw_path', 'raw_sha256'):
        if key not in row or key not in item or item[key] != row[key]:
            raise ValueError('intake differs from canonical identity:' + key)
    evidence=deepcopy(row)
    for key in ('candidate_source','evidence_tier','instrumentation_identity','internal_contract','timestamp','provenance','observed_effect'):
        evidence[key]=deepcopy(item[key])
    evidence.update(oracle_verdict='fail',rule_id=item['internal_contract'],primary_rule_violated=False)
    evidence['endpoint_identity']={r:deepcopy(row['typed_facts'][r]) for r in ('writer','reader')}
    evidence['provenance']['canonical_execution_raw_sha256']=row['raw_sha256']
    evidence['provenance']['blackbox_task']=deepcopy(row['instrumentation_observation']['blackbox_task'])
    return evidence


def observation_links(rows):
    """Link executed same-input blackbox rows without rewriting planned tasks."""
    rows=list(rows); links=[]
    for row in rows:
        qualified=row.get("instrumentation_observation",{})
        if qualified.get("qualification")!="qualified":continue
        matches=[]; rejected=[]
        for other in rows:
            if other.get("instrumentation_observation",{}).get("qualification")!="blackbox_observed":continue
            if other.get("execution_stage")!="judged" or other.get("actual_run") is not True:continue
            keys=("effective_input_sha256","target_id","target_revision","rmw","arm","repeat","source_manifest")
            if all(row.get(k)==other.get(k) for k in keys) and row.get("run_id")!=other.get("run_id") and row.get("domain_id")!=other.get("domain_id") and row.get("raw_path")!=other.get("raw_path"):
                try:
                    if _semantic_profiles(qualified, row.get('domain_id')) != _semantic_profiles(other['instrumentation_observation'], other.get('domain_id')):
                        raise ValueError('non_tracing_transport_profile_mismatch')
                except (ValueError, KeyError, TypeError, AttributeError, ET.ParseError) as exc:
                    rejected.append({'run_id': other.get('run_id'), 'cell_id': other.get('cell_id'),
                                     'reason': str(exc)})
                    continue
                matches.append(other)
        status = ('executed' if len(matches)==1 else 'ambiguous' if len(matches)>1 else
                  'profile_mismatch' if rejected else 'not_run')
        reason = ('same_input_and_non_tracing_profiles_verified' if status=='executed' else
                  'multiple_same_input_blackbox_executions' if status=='ambiguous' else
                  'non_tracing_transport_profile_rejected' if status=='profile_mismatch' else
                  'same_input_blackbox_not_run')
        links.append({"origin_run_id":row["run_id"],"origin_cell_id":row["cell_id"],
                      "status":status,
                      "reason":reason,
                      "rejected_blackbox_executions":rejected,
                      "effective_input_sha256":row.get("effective_input_sha256"),
                      "blackbox_executions":[{k:o.get(k) for k in ("run_id","cell_id","domain_id","raw_path","raw_sha256","oracle_verdict")} for o in matches],
                      "candidate_confirmation":"not_performed", "planned_task_unchanged":True})
    return links
