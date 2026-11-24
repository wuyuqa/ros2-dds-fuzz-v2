"""Public runner -> sealed group carrier -> persisted application audit (offline).

The pipe/process fixture is authored protocol data, never live DDS evidence.
"""
from copy import deepcopy
import json
import hashlib

import pytest
from pathlib import Path

from r2f.campaign import runner
from r2f.campaign.corpus import topology_corpus_from_profile
from r2f.campaign.feedback import FeedbackLog
from r2f.campaign.scheduler import ExploratoryScheduler, required_catalog_hash
from r2f.modules.s1 import fuzz_mutators_v2, input_model
from r2f.runtime.topology_route import run_topology_route
from r2f.runtime.topology_worker import OfflineTopologyBackend
from test_topology_cp3_consumer import cp3_program, transport  # noqa: F401
from test_topology_participant_control_multi import transport as process_transport  # noqa: F401
from test_topology_participant_control_worker import prepared_backend
from test_topology_cycle import profile
from test_topology_participant_control_multi import multi_program
from test_s1_topology_model import action
from r2f.runtime.topology_route import audit_topology_application


class GroupProtocolFixture(OfflineTopologyBackend):
    """Offline carrier using the existing authored OS/pipe boundary fixture."""

    def __init__(self, directory, config):
        super().__init__()
        protocol = prepared_backend(directory, config['topology_program'])
        protocol.real_ros = False
        protocol.config.update(run_id=config['run_id'], domain_id=config['domain_id'])
        # Forward the carrier interface only; retain offline provenance/admission.
        for method in ('prepare', 'set_event_sink', 'create_endpoint', 'destroy_endpoint',
                       'snapshot', 'publish_batch', 'pump', 'close'):
            setattr(self, method, getattr(protocol, method))


def run_report(directory, program, operator='queue.depth'):
    doc = profile(1, operators=(operator,))
    doc['rmws'] = ['rmw_cyclonedds_cpp']
    source = fuzz_mutators_v2.corpus_v2()
    entry = next(e for e in source['entries'] if e['seed_id'] == 'topology.multi_endpoint')
    entry['document'] = deepcopy(program)
    entry['content_sha256'] = input_model.content_hash(program)
    corpus = topology_corpus_from_profile(doc, source, cycle_id=doc['cycles'][0]['cycle_id'])
    feedback = FeedbackLog()
    scheduler = ExploratoryScheduler(corpus, feedback, rng_seed=17, warmup=0)

    def offline(config, output):
        backend = GroupProtocolFixture(output / 'fixture-carrier', config)
        return run_topology_route(config, output, backend=backend)

    catalog = required_catalog_hash()
    cycle = runner.run_exploratory_cycle(directory / 'cycle', corpus=corpus,
        feedback=feedback, scheduler=scheduler, profile=doc, max_inputs=1,
        runtime=offline, execute=True, run_id='offline-group-application',
        domain_id=173, repo=Path.cwd())
    row = cycle['rows'][0]
    saved = json.loads((Path(row['runtime_config_path']).parent / 'row.json').read_text())
    report = json.loads(Path(cycle['report_path']).read_text())
    runtime = json.loads(Path(row['runtime_result_path']).read_text())
    assert saved == report['rows'][0]
    assert not saved['actual_run'] and not saved['candidate_eligible'] and not saved['seal_eligible']
    assert saved['execution_stage'] == 'aborted' and saved['target_revision'] == 0
    assert runtime['fixture'] and not runtime['real_execution']
    assert not any(runtime['mutation_receipt'].values())
    assert required_catalog_hash() == catalog
    return runtime, saved


def test_full_cp3_queue_depth_observed_through_public_runner_without_execution_credit(tmp_path, transport):
    runtime, row = run_report(tmp_path, cp3_program())
    audit = runtime['application_audit']
    assert audit['status'] == 'fixture_application_observed', audit
    assert audit['layers'][0]['operator_id'] == 'queue.depth'
    assert audit['layers'][0]['status'] == 'fixture_application_observed'
    assert row['typed_facts']['diagnostic_valid'], row['typed_facts']
    emitted = [e for e in runtime['events'] if e['event'] == 'topology_application_audited']
    assert len(emitted) == 1 and emitted[0]['status'] == audit['status']
    # Six physical retirements, each with both worker and controller receipts.
    retired = [e for e in runtime['events'] if e['event'] == 'topology_endpoint_destroyed']
    assert len(retired) == 6
    refs = {r['event_index'] for r in audit['layers'][0]['raw_refs']}
    assert {e['event_index'] for e in retired} <= refs


def lifecycle_program(implicit=False):
    program = multi_program(1, 1)
    if not implicit:
        program['actions'].append(action('retire', 'destroy', 'r0', 1))
    program['actions'].append(action('reborn', 'recreate', 'r0', 2))
    program['actions'] += [action('idle_' + str(i), 'snapshot') for i in range(12)]
    return program


@pytest.mark.parametrize('implicit', [False, True], ids=['explicit', 'implicit'])
def test_group_lifecycle_actions_are_observed_with_retained_process(tmp_path, transport, implicit):
    # action.delete audits EVERY surviving action, including destroy (unlike QoS).
    runtime, _ = run_report(tmp_path, lifecycle_program(implicit), operator='action.delete')
    audit = runtime['application_audit']
    assert audit['status'] == 'fixture_application_observed', audit
    created = [e for e in runtime['events']
               if e['event'] == 'endpoint_created' and e.get('logical_id') == 'r0']
    assert len(created) == 2
    assert created[0]['pid'] == created[1]['pid']
    assert created[0]['context_instance_id'] == created[1]['context_instance_id']
    assert created[0]['worker_instance_id'] == created[1]['worker_instance_id']
    assert created[0]['endpoint_gid'] != created[1]['endpoint_gid']
    assert created[0]['endpoint_generation'] != created[1]['endpoint_generation']
    refs = {r['event_index'] for r in audit['layers'][0]['raw_refs']}
    chain = [e for e in runtime['events'] if e.get('logical_id') == 'r0'
             and e.get('generation') == 1 and (e['event'] in
             {'endpoint_destroyed', 'topology_endpoint_destroyed'}
             or e['event'] == 'command_done' and e.get('command') == 'destroy_endpoint')]
    assert len(chain) == 3 and {e['event_index'] for e in chain} <= refs


def corrupt_copy(runtime, row, directory, change):
    """Corrupt ONLY a new offline artifact; retain the public runner's original."""
    events = deepcopy(runtime['events'])
    change(events)
    for index, event in enumerate(events):
        event['event_index'] = index
    directory.mkdir()
    raw = directory / 'damaged-fixture.jsonl'
    raw.write_text(''.join(json.dumps(e) + '\n' for e in events))
    audit = audit_topology_application(row['mutation_plan'], row['input_layer_receipts'],
        {'raw_path': str(raw), 'raw_sha256': hashlib.sha256(raw.read_bytes()).hexdigest()})
    (directory / 'application-audit.json').write_text(json.dumps(audit, indent=2) + '\n')
    return audit


def lifecycle_rows(events):
    def one(kind, **fields):
        return next(e for e in events if e['event'] == kind
                    and all(e.get(k) == v for k, v in fields.items()))
    return {
        'request': one('group_command_requested', command='destroy_endpoint', logical_id='r0'),
        'cleanup': one('endpoint_destroyed', logical_id='r0', generation=1),
        'ack': one('command_done', command='destroy_endpoint', logical_id='r0'),
        'controller': one('topology_endpoint_destroyed', logical_id='r0'),
        'create_request': one('group_command_requested', command='create_endpoint', logical_id='r0', generation=2),
        'created': one('endpoint_created', logical_id='r0', generation=2),
        'begin': one('topology_action_begin', action_id='reborn'),
        'done': one('topology_action_done', action_id='reborn'),
    }


@pytest.mark.parametrize('implicit', [False, True], ids=['explicit', 'implicit'])
def test_corrupt_group_lifecycles_fail_closed_without_future_proof(tmp_path, transport, implicit):
    runtime, row = run_report(tmp_path, lifecycle_program(implicit), operator='action.delete')
    assert runtime['application_audit']['status'] == 'fixture_application_observed'
    original = Path(runtime['raw_path']).read_bytes()
    cases = []

    def edit(part, field, value):
        def change(events):
            lifecycle_rows(events)[part][field] = value
        return change

    for part in ('request', 'cleanup', 'ack', 'controller'):
        for field, value in (('generation', 99), ('pid', 999999),
                             ('context_instance_id', 'wrong-context'),
                             ('worker_instance_id', 'wrong-worker')):
            cases.append((part + '-' + field, edit(part, field, value)))
        cases.append(('missing-' + part, lambda events, part=part:
                      events.remove(lifecycle_rows(events)[part])))
        cases.append(('duplicate-' + part, lambda events, part=part:
                      events.insert(events.index(lifecycle_rows(events)[part]),
                                    deepcopy(lifecycle_rows(events)[part]))))
    for part in ('request', 'ack', 'controller'):
        for value in (None, True, -1, '28', 999999):
            cases.append((part + '-cid-' + repr(value), edit(part, 'command_id', value)))
    for part in ('request', 'cleanup', 'ack', 'controller'):
        cases.append((part + '-unknown-command', edit(part, 'command', 'unknown_operation')))
    for part in ('cleanup', 'ack', 'controller'):
        cases.append((part + '-gid', edit(part, 'endpoint_gid', 'f' * 32)))
        cases.append((part + '-unclosed', edit(part, 'cleanup_status', 'unclosed')))
    for field in ('pid', 'process_start_ticks', 'context_instance_id', 'worker_instance_id'):
        cases.append(('replacement-' + field, edit('created', field, 'different-identity')))
    for field in ('endpoint_gid', 'endpoint_generation'):
        def reused_identity(events, field=field):
            rows = lifecycle_rows(events)
            prior = next(e for e in events if e['event'] == 'endpoint_created'
                         and e.get('logical_id') == 'r0' and e.get('generation') == 1)
            rows['created'][field] = rows['done']['receipt']['after'][field] = prior[field]
        cases.append(('reused-' + field, reused_identity))

    def missing_create_ack(events):
        request = lifecycle_rows(events)['create_request']
        ack = next(e for e in events if e['event'] == 'command_done'
                   and e.get('command_id') == request['command_id']
                   and e.get('group_id') == request['group_id'])
        events.remove(ack)
    cases.append(('missing-create-ack', missing_create_ack))

    def unknown_action(events):
        rows = lifecycle_rows(events)
        for action in (rows['begin']['action'], rows['done']['action'],
                       rows['done']['receipt']['action']):
            action['op'] = 'unknown_operation'
    cases.append(('unknown-action', unknown_action))
    cases.append(('unsealed-optin', lambda events:
                  next(e for e in events if e['event'] == 'input_binding')['receipt']
                  .pop('participant_control')))

    def move_after(part, later):
        def change(events):
            rows = lifecycle_rows(events)
            event = rows[part]
            events.remove(event)
            events.insert(events.index(rows[later]) + 1, event)
            event['monotonic_ns'] = rows[later]['monotonic_ns']
        return change

    for part in ('ack', 'controller'):
        cases.append(('future-' + part, move_after(part, 'created')))
    cases.append(('done-before-create', lambda events: lifecycle_rows(events)['done']
                  .update(monotonic_ns=lifecycle_rows(events)['create_request']['monotonic_ns'] - 1)))

    def bad_before(events):
        rows = lifecycle_rows(events)
        rows['begin']['before']['generation'] = 99
        rows['done']['receipt']['before']['generation'] = 99
    cases.append(('wrong-before-generation', bad_before))
    for field, value in (('generation', 99), ('alive', False), ('pid', 999999),
                         ('endpoint_gid', 'f' * 32), ('endpoint_generation', 'forged')):
        cases.append(('after-' + field, lambda events, field=field, value=value:
                      lifecycle_rows(events)['done']['receipt']['after'].update({field: value})))

    if not implicit:
        def future_destroy_done(events):
            rows = lifecycle_rows(events)
            done = next(e for e in events if e['event'] == 'topology_action_done'
                        and e['action_id'] == 'retire')
            events.remove(done)
            events.insert(events.index(rows['begin']) + 1, done)
            done['monotonic_ns'] = rows['begin']['monotonic_ns']
        cases.append(('future-explicit-action-done', future_destroy_done))
        for field, value in (('generation', 99), ('alive', False)):
            def destroy_before(events, field=field, value=value):
                begin = next(e for e in events if e['event'] == 'topology_action_begin'
                             and e['action_id'] == 'retire')
                done = next(e for e in events if e['event'] == 'topology_action_done'
                            and e['action_id'] == 'retire')
                begin['before'][field] = done['receipt']['before'][field] = value
            cases.append(('destroy-before-' + field, destroy_before))
        for field, value in (('generation', 99), ('alive', True)):
            def destroy_after(events, field=field, value=value):
                done = next(e for e in events if e['event'] == 'topology_action_done'
                            and e['action_id'] == 'retire')
                done['receipt']['after'][field] = value
            cases.append(('destroy-after-' + field, destroy_after))
    else:
        def early_action_begin(events):
            rows = lifecycle_rows(events)
            rows['begin']['monotonic_ns'] = rows['request']['monotonic_ns'] + 1
        cases.append(('destroy-request-before-action', early_action_begin))

    accepted = []
    for name, change in cases:
        audit = corrupt_copy(runtime, row, tmp_path / name, change)
        if audit['status'] in {'application_observed', 'fixture_application_observed',
                                'artifact_application_observed'}:
            accepted.append(name)
        assert not audit['execution_evidence'] and not any(audit['mutation_receipt'].values())
    assert not accepted, accepted
    assert Path(runtime['raw_path']).read_bytes() == original


def test_queue_depth_requires_destroy_action_state_not_only_the_recreate_chain(tmp_path, transport):
    runtime, row = run_report(tmp_path, lifecycle_program())
    assert runtime['application_audit']['status'] == 'fixture_application_observed'

    def corrupt_destroy_after(events):
        done = next(e for e in events if e['event'] == 'topology_action_done'
                    and e['action_id'] == 'retire')
        done['receipt']['after']['alive'] = True

    audit = corrupt_copy(runtime, row, tmp_path / 'destroy-after-still-alive', corrupt_destroy_after)
    assert audit['status'] not in {'application_observed', 'fixture_application_observed',
                                    'artifact_application_observed'}, audit
