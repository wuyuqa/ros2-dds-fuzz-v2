"""CP3 authored offline protocol -> persisted runtime -> canonical consumers.

The OS/pipe fixture supplies a transcript, never execution/candidate/seal credit.
"""
from copy import deepcopy
import json
import pytest
from pathlib import Path
import subprocess
from types import SimpleNamespace

_SYSTEM_POPEN = subprocess.Popen

from r2f.campaign.topology_adapter import TopologyFactProducer
from r2f.core.evidence import canonical_hash
from r2f.runtime.topology_pair import run_topology
from test_s1_topology_model import action
from test_topology_participant_control_multi import multi_program, transport as process_transport
from test_topology_participant_control_worker import prepared_backend
from test_topology_runtime_projection_integration import _route_config


@pytest.fixture
def transport(process_transport, monkeypatch):
    """Authored external pipe transcript with per-peer/GID observations."""
    from r2f.runtime import topology_worker as worker
    # Confine the fake pipe/process boundary to the ROS worker; ordinary Git
    # source-manifest reads by the canonical runner must use the real OS API.
    monkeypatch.setattr(worker, 'subprocess', SimpleNamespace(**vars(worker.subprocess)))
    monkeypatch.setattr(subprocess, 'Popen', _SYSTEM_POPEN)
    class Transcript(worker.subprocess.Popen):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            read = self.stdout.read
            def read_pipe():
                import time
                rows = [json.loads(line) for line in read().splitlines()]
                for row in rows:
                    if row['event'] in ('group_context_created', 'group_worker_ready'):
                        row['monotonic_ns'] = time.monotonic_ns()
                return b''.join(json.dumps(row).encode() + b'\n' for row in rows)
            self.stdout.read = read_pipe

        def add(self, event, **fields):
            fields.update(run_id=self.cfg['run_id'], domain_id=self.cfg['domain_id'],
                          observed_domain_id=self.cfg['domain_id'])
            if event == 'send':
                import time
                fields.update(publish_begin_ns=time.monotonic_ns(), publish_return_ns=time.monotonic_ns())
            if event == 'receive':
                fields['reader_gid'] = fields['endpoint_gid']
            if event == 'graph_snapshot':
                from r2f.runtime.topology_worker import endpoint_expressibility
                for kind in ('publishers', 'subscriptions'):
                    for row in fields[kind]:
                        owner, token = next((child, t) for child in process_transport
                            for t in child.tokens.values() if t['endpoint_gid'] == row['endpoint_gid'])
                        spec = next(e for e in owner.cfg['group_members'] if e['logical_id'] == token['logical_id'])
                        row['qos'] = endpoint_expressibility(spec, {})['public_qos']
            if event == 'match_snapshot':
                opposite = 'reader' if fields['worker_role'].startswith('writer') else 'writer'
                peers = [t['endpoint_gid'] for child in process_transport for t in child.tokens.values()
                         if t['worker_role'].startswith(opposite)]
                fields.update(matched_count=len(peers), matched_peer_gids=peers,
                              peer_gids_complete=True, peer_proof_source='dds_matched_endpoint_api')
            return super().add(event, **fields)
    monkeypatch.setattr(worker.subprocess, 'Popen', Transcript)
    return process_transport


def consume(tmp_path, program):
    config = _route_config(program, 'cp3-consumer')
    config.update(run_id='offline-fixture', domain_id=173)
    config['topology_route_binding'] = {
        'schema': 'r2f.topology-route-binding/v1', 'target_id': config['target_id'],
        'capability': config['runtime_capability'], 'plan_hash': config['plan_hash'],
        'final_input_sha256': canonical_hash(config['final_input']),
        'input_layer_receipts_sha256': canonical_hash(config['input_layer_receipts']),
        'effective_input_sha256': config['effective_input_sha256']}
    backend = prepared_backend(tmp_path / 'carrier', config['topology_program'])
    backend.real_ros = False
    runtime = run_topology(config['topology_program'], config, tmp_path / 'runtime', backend=backend)
    assert runtime.get('raw_path'), runtime
    (tmp_path / 'runtime' / 'runtime_result.json').write_text(json.dumps(runtime) + '\n')
    saved = json.loads((tmp_path / 'runtime' / 'runtime_result.json').read_text())
    assert saved == runtime
    raw = Path(saved['raw_path'])
    assert [json.loads(line) for line in raw.read_text().splitlines()] == saved['events']
    facts = TopologyFactProducer().produce({'events': saved['events'], 'runtime': saved, 'config': config})
    (tmp_path / 'consumer.json').write_text(json.dumps(facts, indent=2) + '\n')
    return config, saved, facts


def destroy_program():
    p = multi_program(1, 1)
    p['actions'] += [action('before_destroy', 'snapshot'),
                     action('retire_reader', 'destroy', 'r0', 1)]
    return p


def test_same_destroy_ack_and_controller_receipt_are_consumed(tmp_path, transport):
    config, runtime, facts = consume(tmp_path, destroy_program())
    rows = runtime['events']
    chain = [e for e in rows if e.get('logical_id') == 'r0' and (
        e['event'] == 'group_command_requested' and e.get('command') == 'destroy_endpoint'
        or e['event'] == 'endpoint_destroyed'
        or e['event'] == 'command_done' and e.get('command') == 'destroy_endpoint'
        or e['event'] == 'topology_endpoint_destroyed')]
    assert [e['event'] for e in chain] == [
        'group_command_requested', 'endpoint_destroyed', 'command_done', 'topology_endpoint_destroyed']
    assert 'endpoint_gid' not in chain[0] and 'command_id' not in chain[1]
    assert chain[0]['command_id'] == chain[2]['command_id'] == chain[3]['command_id']
    assert facts['diagnostic_valid'], facts
    assert not facts['execution_evidence'] and not facts['candidate_eligible'] and not facts['seal_eligible']


def test_group_destroy_counts_one_physical_retirement_at_final_boundary(tmp_path, transport):
    _, runtime, facts = consume(tmp_path, destroy_program())
    final = [e for e in runtime['events'] if e['event'] == 'topology_state_boundary'][-1]
    assert final['lifecycle_complete'], final
    reader = next(e for e in facts['state_after']['endpoints'] if e['logical_id'] == 'r0')
    assert reader['generation'] == 1 and reader['liveness'] == 'retired'


def test_closed_destroy_projection_uses_only_the_requested_prefix(tmp_path, transport):
    from r2f.campaign.topology_state_projection import project_topology_state
    config, runtime, _ = consume(tmp_path, destroy_program())
    rows = runtime['events']
    before, after = [e for e in rows if e['event'] == 'topology_state_boundary']
    prefix_path = tmp_path / 'closed-prefix.jsonl'
    prefix_path.write_text(''.join(json.dumps(e) + '\n' for e in rows[:after['event_index'] + 1]))
    def project(path):
        return project_topology_state(config['topology_program'], path,
            before_ref=before['event_index'], after_ref=after['event_index'])
    prefix = project(prefix_path)
    full = project(runtime['raw_path'])
    assert prefix['receipt']['status'] != 'invalid', prefix['receipt']
    assert prefix['state_after'] == full['state_after']
    reader = next(e for e in prefix['state_after']['endpoints'] if e['logical_id'] == 'r0')
    assert reader['generation'] == 1 and reader['liveness'] == 'retired'
    # Changing events beyond the boundary cannot change its lifecycle proof.
    suffix = deepcopy(rows)
    for event in suffix[after['event_index'] + 1:]:
        event['context_instance_id'] = 'unrelated-future-context'
    suffix_path = tmp_path / 'different-suffix.jsonl'
    suffix_path.write_text(''.join(json.dumps(e) + '\n' for e in suffix))
    changed = project(suffix_path)
    assert changed['state_after'] == prefix['state_after']


def cp3_program():
    p = multi_program(2, 2)
    def publish(label, generation0, generation1, start):
        return [action(label + '_w0', 'publish_batch', 'w0', generation0, count=3, start_sequence=start),
                action(label + '_w1', 'publish_batch', 'w1', generation1, count=3, start_sequence=start),
                action(label + '_sentinel_w0', 'publish_batch', 'w0', generation0, count=1, start_sequence=start + 3),
                action(label + '_sentinel_w1', 'publish_batch', 'w1', generation1, count=1, start_sequence=start + 3),
                action(label + '_closed', 'barrier', condition='delivery_sentinel',
                       endpoint_ids=['w0', 'w1', 'r0', 'r1'], timeout_sec=0.05,
                       publish_action_ids=[label + '_sentinel_w0', label + '_sentinel_w1'])]
    p['actions'] += [action('baseline_open', 'snapshot')] + publish('baseline', 1, 1, 0)
    p['actions'] += [action('retire_r0', 'destroy', 'r0', 1),
                     action('reborn_r0', 'recreate', 'r0', 2), action('reader_open', 'snapshot')]
    p['actions'] += publish('reader', 1, 1, 4)
    p['actions'] += [action('retire_w0', 'destroy', 'w0', 1),
                     action('reborn_w0', 'recreate', 'w0', 2), action('writer_open', 'snapshot')]
    p['actions'] += publish('writer', 2, 1, 8)
    for eid, gen in [('w0', 2), ('w1', 1), ('r0', 2), ('r1', 1)]:
        p['actions'].append(action('empty_' + eid, 'destroy', eid, gen))
    p['actions'] += [action('empty_begin', 'snapshot'), action('empty_end', 'snapshot')]
    for eid, gen in [('w0', 3), ('w1', 2), ('r0', 3), ('r1', 2)]:
        p['actions'].append(action('fresh_create_' + eid, 'recreate', eid, gen))
    p['actions'] += [action('fresh_open', 'snapshot')] + publish('fresh', 3, 2, 0)
    return p


EXPECTED = {
    ('w0', 1, 'r0', 1): [0, 1, 2, 3],
    ('w1', 1, 'r0', 1): [0, 1, 2, 3],
    ('w0', 1, 'r1', 1): [0, 1, 2, 3, 4, 5, 6, 7],
    ('w1', 1, 'r1', 1): [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11],
    ('w0', 1, 'r0', 2): [4, 5, 6, 7],
    ('w1', 1, 'r0', 2): [4, 5, 6, 7, 8, 9, 10, 11],
    ('w0', 2, 'r0', 2): [8, 9, 10, 11],
    ('w0', 2, 'r1', 1): [8, 9, 10, 11],
    ('w0', 3, 'r0', 3): [0, 1, 2, 3],
    ('w0', 3, 'r1', 2): [0, 1, 2, 3],
    ('w1', 2, 'r0', 3): [0, 1, 2, 3],
    ('w1', 2, 'r1', 2): [0, 1, 2, 3],
}


def pair_key(row):
    return row['writer'], row['writer_generation'], row['reader'], row['reader_generation']


def test_every_historical_delivery_is_refreshed_including_stale_complete(tmp_path, transport):
    _, runtime, _ = consume(tmp_path, cp3_program())
    deliveries = {pair_key(row): row for row in runtime['delivery_facts']}
    assert {key: row['expected_sequences'] for key, row in deliveries.items()} == EXPECTED
    assert {key: row['received_sequences'] for key, row in deliveries.items()} == EXPECTED
    assert all(row['status'] == 'complete' for row in deliveries.values())
    assert sum(len(row['received_sequences']) for row in deliveries.values()) == 64


def test_canonical_group_history_accounts_for_every_then_live_reader(tmp_path, transport):
    _, runtime, facts = consume(tmp_path, cp3_program())
    assert facts['diagnostic_valid'], facts
    assert facts['unresolved_historical_obligations'] == [], facts['unresolved_historical_obligations']
    windows = facts['historical_participant_group_windows']
    assert len(windows) == 12 and all(w['window_projection_valid'] for w in windows)
    assert len(facts['historical_live_windows']) == 4
    assert all(w['scope'] == 'participant_control_verified_retirement_historical_window' for w in windows)
    assert len(facts['historical_send_coverage']) == 24
    covered = {c['send_ref']['event_index'] for c in facts['historical_send_coverage']}
    current = [e for e in runtime['events'] if e['event'] == 'send'
               and (e['logical_id'], e['generation']) in {('w0', 3), ('w1', 2)}]
    assert len(current) == 8 and all(e['event_index'] not in covered for e in current)
    assert not runtime['real_execution'] and not facts['candidate_eligible']


def test_historical_multi_writer_sample_cannot_borrow_final_single_writer_inventory(
        tmp_path, transport, monkeypatch):
    from r2f.runtime import topology_worker as worker
    class NoSampleIdentity(worker.subprocess.Popen):
        def add(self, event, **fields):
            if event == 'receive':
                fields = {k: v for k, v in fields.items()
                          if not k.startswith('payload_') and k != 'string_data'}
            return super().add(event, **fields)
    monkeypatch.setattr(worker.subprocess, 'Popen', NoSampleIdentity)
    p = multi_program(2, 1)
    p['actions'] += [action('open_multi_writer', 'snapshot'),
                     action('only_w0_sends', 'publish_batch', 'w0', 1, count=2, start_sequence=20),
                     action('retire_other_writer', 'destroy', 'w1', 1),
                     action('now_single_writer', 'snapshot')]
    _, runtime, facts = consume(tmp_path, p)
    old = next(row for row in runtime['delivery_facts'] if row['writer'] == 'w0')
    assert old['expected_sequences'] == [20, 21]
    assert old['single_writer_receipts'] == 0, old
    assert old['received_sequences'] == []
    assert old['status'] == 'unknown'
    assert not facts['candidate_eligible'] and not facts['seal_eligible']


def reconsume_fixture(config, runtime, events):
    """Damage only this test's authored raw; never touches historical evidence."""
    import hashlib
    raw = Path(runtime['raw_path'])
    raw.write_text(''.join(json.dumps(e) + '\n' for e in events))
    saved = deepcopy(runtime)
    saved.update(events=events, raw_sha256=hashlib.sha256(raw.read_bytes()).hexdigest())
    (raw.parent / 'runtime_result.json').write_text(json.dumps(saved) + '\n')
    return TopologyFactProducer().produce({'events': events, 'runtime': saved, 'config': config})


def test_missing_historical_group_close_stays_unknown_without_crashing(tmp_path, transport):
    config, runtime, _ = consume(tmp_path, cp3_program())
    rows = deepcopy(runtime['events'])
    close = next(e for e in rows if e['event'] == 'topology_delivery_window_closed'
                 and e['writer'] == 'w1' and e['reader'] == 'r1')
    missing_open = close['open_ref']
    close['event'] = 'fixture_omitted_window_close'
    facts = reconsume_fixture(config, runtime, rows)
    windows = facts['historical_participant_group_windows']
    missing = next(w for w in windows if w['open_ref']['event_index'] == missing_open)
    assert missing['close_ref'] is None
    assert not missing['window_projection_valid']
    assert facts['unresolved_historical_obligations']
    assert not facts['candidate_eligible'] and not facts['seal_eligible']


def test_one_group_close_cannot_certify_another_close_after_destroy(tmp_path, transport):
    config, runtime, _ = consume(tmp_path, cp3_program())
    rows = deepcopy(runtime['events'])
    close = [e for e in rows if e['event'] == 'topology_delivery_window_closed'
             and e['writer'] == 'w1' and e['writer_generation'] == 1
             and e['reader'] == 'r1' and e['reader_generation'] == 1][-1]
    late_slot = next(e for e in rows if e['event'] == 'topology_pair_observation'
                     and e['step_id'] == 'snapshot:empty_begin')
    late = deepcopy(close)
    for field in ('event_index', 'monotonic_ns', 'time_ns'):
        late[field] = late_slot[field]
    close['event'] = 'fixture_delayed_window_close'
    rows[late_slot['event_index']] = late
    facts = reconsume_fixture(config, runtime, rows)
    windows = facts['historical_participant_group_windows']
    row = next(w for w in windows if w.get('close_ref', {}).get('event_index') == late['event_index'])
    assert not row['window_projection_valid'], row
    assert row['delivery_closure'] == 'unknown'
    assert row['destroy_operation_refs'] == []
    assert facts['unresolved_historical_obligations']


@pytest.mark.parametrize('cid', [None, True, -1, '28'])
def test_prefix_destroy_needs_an_actual_command_id(tmp_path, transport, cid):
    from r2f.campaign.topology_state_projection import project_topology_state
    config, runtime, _ = consume(tmp_path, destroy_program())
    rows = deepcopy(runtime['events'])
    before, after = [e for e in rows if e['event'] == 'topology_state_boundary']
    for e in rows:
        if e.get('logical_id') == 'r0' and (
            e['event'] == 'group_command_requested' and e.get('command') == 'destroy_endpoint'
            or e['event'] == 'command_done' and e.get('command') == 'destroy_endpoint'
            or e['event'] == 'topology_endpoint_destroyed'):
            e['command_id'] = cid
    raw = tmp_path / 'bad-cid-prefix.jsonl'
    raw.write_text(''.join(json.dumps(e) + '\n' for e in rows[:after['event_index'] + 1]))
    projected = project_topology_state(config['topology_program'], raw,
        before_ref=before['event_index'], after_ref=after['event_index'])
    reader = next(e for e in projected['state_after']['endpoints'] if e['logical_id'] == 'r0')
    assert reader['liveness'] == 'unknown', reader


def test_offline_runner_persists_cp3_history_in_its_existing_report(tmp_path, transport):
    row, runtime = run_group_report(tmp_path, cp3_program())
    facts = row['typed_facts']
    assert facts['diagnostic_valid'], facts.get('unknown_reasons')
    assert facts['unresolved_historical_obligations'] == []
    assert len(facts['historical_send_coverage']) == 24
    assert len(facts['historical_participant_group_windows']) == 12
    assert len(facts['historical_live_windows']) == 4
    assert row['oracle_verdict'] == 'pass'
    assert {pair_key(d): d['received_sequences'] for d in runtime['delivery_facts']} == EXPECTED
    assert len(runtime['delivery_facts']) == 12
    assert sum(len(d['received_sequences']) for d in runtime['delivery_facts']) == 64
    historical = {c['send_ref']['event_index'] for c in facts['historical_send_coverage']}
    current = {e['event_index'] for e in runtime['events'] if e['event'] == 'send'
               and (e['logical_id'], e['generation']) in {('w0', 3), ('w1', 2)}}
    assert len(current) == 8 and not historical & current
    assert row['target_revision'] == 0
    assert not row['actual_run'] and not row['candidate_eligible'] and not row['seal_eligible']
    assert facts['fixture'] and not facts['execution_evidence']
    assert row['execution_stage'] == 'aborted'


def test_retired_writer_can_still_have_a_verified_receipt_at_live_reader(
        tmp_path, transport, monkeypatch):
    from r2f.runtime import topology_worker as worker
    pipe_type = worker.subprocess.Popen
    pending = []
    class DelayedReceipts(pipe_type):
        def add(self, event, **fields):
            if event == 'receive':
                pending.append((self, deepcopy(fields)))
                return
            result = super().add(event, **fields)
            if event == 'command_done' and fields.get('command') == 'destroy_endpoint':
                for child, sample in pending:
                    pipe_type.add(child, 'receive', **sample)
                pending.clear()
            return result
    monkeypatch.setattr(worker.subprocess, 'Popen', DelayedReceipts)
    p = multi_program(1, 1)
    p['actions'] += [action('open', 'snapshot'),
                     action('send_before_retirement', 'publish_batch', 'w0', 1, count=2, start_sequence=0),
                     action('retire_writer', 'destroy', 'w0', 1), action('reader_still_alive', 'snapshot')]
    _, runtime, facts = consume(tmp_path, p)
    rows = runtime['events']
    retirement = next(e for e in rows if e['event'] == 'endpoint_destroyed' and e['logical_id'] == 'w0')
    samples = [e for e in rows if e['event'] == 'receive']
    assert len(samples) == 2 and all(e['event_index'] > retirement['event_index'] for e in samples)
    delivery = runtime['delivery_facts'][0]
    assert delivery['expected_sequences'] == delivery['received_sequences'] == [0, 1]
    assert delivery['status'] == 'complete'
    assert len(facts['deliveries']) == 2
    assert not facts['candidate_eligible']


def test_same_nanosecond_lifetimes_follow_raw_order_and_not_future_generations(
        tmp_path, transport, monkeypatch):
    import time
    fixed = time.monotonic_ns()
    monkeypatch.setattr(time, 'monotonic_ns', lambda: fixed)
    # Equal clocks exercise the public supporting delivery rows only. The
    # canonical protocol intentionally retains its stricter chronology gate.
    _, runtime, _ = consume(tmp_path, cp3_program())
    assert {pair_key(d): d['expected_sequences'] for d in runtime['delivery_facts']} == EXPECTED
    assert {pair_key(d): d['received_sequences'] for d in runtime['delivery_facts']} == EXPECTED


def test_early_boundary_cannot_borrow_a_future_destroy_ack(tmp_path, transport):
    from r2f.campaign.topology_state_projection import project_topology_state
    config, runtime, _ = consume(tmp_path, destroy_program())
    rows = deepcopy(runtime['events'])
    before, after = [e for e in rows if e['event'] == 'topology_state_boundary']
    ack = next(e for e in rows if e['event'] == 'command_done' and e.get('command') == 'destroy_endpoint')
    future_ack = deepcopy(ack)
    future_ack.update(event_index=len(rows), monotonic_ns=rows[-1]['monotonic_ns'] + 1)
    ack['event'] = 'fixture_ack_not_yet_received'
    rows.append(future_ack)
    raw = tmp_path / 'future-ack.jsonl'
    raw.write_text(''.join(json.dumps(e) + '\n' for e in rows))
    projection = project_topology_state(config['topology_program'], raw,
        before_ref=before['event_index'], after_ref=after['event_index'])
    reader = next(e for e in projection['state_after']['endpoints'] if e['logical_id'] == 'r0')
    assert reader['liveness'] == 'unknown'


@pytest.mark.parametrize('part,field,value', [
    ('request', 'group_id', 'wrong-group'),
    ('cleanup', 'endpoint_gid', 'f' * 32),
    ('cleanup', 'cleanup_status', 'unclosed'),
    ('ack', 'pid', 999999),
    ('ack', 'context_instance_id', 'wrong-context'),
    ('ack', 'endpoint_generation', 'wrong-worker-incarnation'),
    ('ack', 'generation', 2),
    ('ack', 'endpoint_gid', 'e' * 32),
    ('ack', 'command_id', 999999),
    ('ack', 'monotonic_ns', 1),
    ('controller', 'command_id', 999999),
    ('controller', 'cleanup_status', 'unclosed'),
])
def test_destroy_proof_never_waives_another_command_or_identity(
        tmp_path, transport, part, field, value):
    from r2f.campaign.topology_state_projection import project_topology_state
    config, runtime, _ = consume(tmp_path, destroy_program())
    rows = deepcopy(runtime['events'])
    chain = {
        'request': next(e for e in rows if e['event'] == 'group_command_requested'
                        and e.get('command') == 'destroy_endpoint'),
        'cleanup': next(e for e in rows if e['event'] == 'endpoint_destroyed' and e['logical_id'] == 'r0'),
        'ack': next(e for e in rows if e['event'] == 'command_done' and e.get('command') == 'destroy_endpoint'),
        'controller': next(e for e in rows if e['event'] == 'topology_endpoint_destroyed'),
    }
    chain[part][field] = value
    facts = reconsume_fixture(config, runtime, rows)
    assert not facts['diagnostic_valid'] and facts['unknown_reasons']
    before, after = [e for e in rows if e['event'] == 'topology_state_boundary']
    projected = project_topology_state(config['topology_program'], runtime['raw_path'],
        before_ref=before['event_index'], after_ref=after['event_index'])
    reader = next(e for e in projected['state_after']['endpoints'] if e['logical_id'] == 'r0')
    assert reader['liveness'] == 'unknown'


@pytest.mark.parametrize('event_kind', ['send', 'receive', 'graph_snapshot', 'topology_endpoint_destroyed'])
def test_destroy_receipt_is_no_general_retired_activity_waiver(tmp_path, transport, event_kind):
    config, runtime, _ = consume(tmp_path, destroy_program())
    rows = deepcopy(runtime['events'])
    controller = next(e for e in rows if e['event'] == 'topology_endpoint_destroyed')
    slot = next(e for e in rows if e['event'] == 'topology_pair_observation'
                and e['event_index'] > controller['event_index'])
    forged = deepcopy(controller)
    for field in ('event_index', 'monotonic_ns', 'time_ns'):
        forged[field] = slot[field]
    forged['event'] = event_kind
    rows[slot['event_index']] = forged
    facts = reconsume_fixture(config, runtime, rows)
    assert not facts['diagnostic_valid'] and facts['unknown_reasons']
    assert not facts['candidate_eligible'] and not facts['seal_eligible']


def test_prefix_rejects_duplicate_controller_retirement_for_one_destroy(tmp_path, transport):
    from r2f.campaign.topology_state_projection import project_topology_state
    config, runtime, _ = consume(tmp_path, destroy_program())
    rows = deepcopy(runtime['events'])
    before, after = [e for e in rows if e['event'] == 'topology_state_boundary']
    controller = next(e for e in rows if e['event'] == 'topology_endpoint_destroyed')
    rows.insert(controller['event_index'] + 1, deepcopy(controller))
    for index, row in enumerate(rows):
        row['event_index'] = index
    raw = tmp_path / 'duplicate-controller.jsonl'
    raw.write_text(''.join(json.dumps(e) + '\n' for e in rows))
    projected = project_topology_state(config['topology_program'], raw,
        before_ref=before['event_index'], after_ref=after['event_index'])
    reader = next(e for e in projected['state_after']['endpoints'] if e['logical_id'] == 'r0')
    assert reader['liveness'] == 'unknown'


def test_incomplete_raw_boundary_is_never_upgraded_from_complete_sidecar(tmp_path, transport):
    config, runtime, _ = consume(tmp_path, cp3_program())
    rows = deepcopy(runtime['events'])
    after = [e for e in rows if e['event'] == 'topology_state_boundary'][-1]
    after['lifecycle_complete'] = False
    # An optimistic summary is not authority over immutable raw proof flags.
    for d in runtime['delivery_facts']:
        d.update(status='complete', expected_sequences=[], received_sequences=[], missing_sequences=[])
    runtime['pair_facts'] = []
    facts = reconsume_fixture(config, runtime, rows)
    assert facts['diagnostic_valid'], facts['unknown_reasons']
    assert facts['historical_participant_group_windows'] == []
    assert len(facts['unresolved_historical_obligations']) == 32
    assert all(e['liveness'] == 'unknown' for e in facts['state_after']['endpoints'])
    saved = [json.loads(line) for line in Path(runtime['raw_path']).read_text().splitlines()]
    assert saved[after['event_index']]['lifecycle_complete'] is False
    assert not facts['candidate_eligible'] and not facts['seal_eligible']


def mixed_reader_program(readers=2):
    """One writer keeps serving readers after r0's verified retirement."""
    program = multi_program(1, readers)
    program['actions'] += [action('baseline_open', 'snapshot')]
    program['actions'] += [
        action('batch', 'publish_batch', 'w0', 1, count=3, start_sequence=0),
        action('sentinel', 'publish_batch', 'w0', 1, count=1, start_sequence=3),
        action('closed', 'barrier', condition='delivery_sentinel',
               endpoint_ids=['w0'] + [f'r{i}' for i in range(readers)],
               timeout_sec=0.05, publish_action_ids=['sentinel']),
        action('retire_reader', 'destroy', 'r0', 1),
    ]
    return program


def run_group_report(tmp_path, program, *, damage=None):
    """Submit public input and read the canonical persisted row and report."""
    import hashlib
    from r2f.campaign import runner
    from r2f.campaign.corpus import topology_corpus_from_profile
    from r2f.campaign.feedback import FeedbackLog
    from r2f.campaign.scheduler import ExploratoryScheduler, required_catalog_hash
    from r2f.modules.s1 import fuzz_mutators_v2, input_model
    from test_topology_cycle import profile

    doc = profile(1)
    doc['rmws'] = ['rmw_cyclonedds_cpp']
    source = fuzz_mutators_v2.corpus_v2()
    entry = next(e for e in source['entries'] if e['seed_id'] == 'topology.multi_endpoint')
    entry['document'] = program
    entry['content_sha256'] = input_model.content_hash(program)
    corpus = topology_corpus_from_profile(doc, source, cycle_id=doc['cycles'][0]['cycle_id'])
    feedback = FeedbackLog()
    scheduler = ExploratoryScheduler(corpus, feedback, rng_seed=17, warmup=0)

    def offline(config, directory):
        backend = prepared_backend(directory / 'fixture-carrier', config['topology_program'])
        backend.real_ros = False
        backend.config.update(run_id=config['run_id'], domain_id=config['domain_id'])
        cfg = deepcopy(config)
        cfg['topology_route_binding'] = {
            'schema': 'r2f.topology-route-binding/v1', 'target_id': cfg['target_id'],
            'capability': cfg['runtime_capability'], 'plan_hash': cfg['plan_hash'],
            'final_input_sha256': canonical_hash(cfg['final_input']),
            'input_layer_receipts_sha256': canonical_hash(cfg['input_layer_receipts']),
            'effective_input_sha256': cfg['effective_input_sha256']}
        runtime = run_topology(cfg['topology_program'], cfg, directory, backend=backend)
        assert not runtime['real_execution']
        if damage is not None:
            damage(runtime['events'])
            raw = Path(runtime['raw_path'])
            raw.write_text(''.join(json.dumps(e) + '\n' for e in runtime['events']))
            runtime['raw_sha256'] = hashlib.sha256(raw.read_bytes()).hexdigest()
        return runtime

    catalog = required_catalog_hash()
    result = runner.run_exploratory_cycle(tmp_path / 'cycle', corpus=corpus, feedback=feedback,
        scheduler=scheduler, profile=doc, max_inputs=1, runtime=offline, execute=True,
        run_id='offline-cp3-mixed-readers-fixture', domain_id=173, repo=Path.cwd())
    row = result['rows'][0]
    saved = json.loads((Path(row['runtime_config_path']).parent / 'row.json').read_text())
    report = json.loads(Path(result['report_path']).read_text())
    assert saved['typed_facts'] == report['rows'][0]['typed_facts'] == row['typed_facts']
    assert saved['oracle_verdict'] == report['rows'][0]['oracle_verdict'] == row['oracle_verdict']
    assert not saved['actual_run'] and not saved['candidate_eligible'] and not saved['seal_eligible']
    assert saved['typed_facts']['fixture'] and not saved['typed_facts']['execution_evidence']
    assert saved['execution_stage'] == 'aborted' and saved['target_revision'] == 0
    assert required_catalog_hash() == catalog
    runtime = json.loads(Path(row['runtime_result_path']).read_text())
    return saved, runtime


def test_public_runner_current_reader_cannot_waive_retired_reader_missing_close(tmp_path, transport):
    def omit_close(events):
        for event in events:
            if event['event'] == 'topology_delivery_window_closed' and event['reader'] == 'r0':
                event['event'] = 'fixture_omitted_window_close'

    row, runtime = run_group_report(tmp_path, mixed_reader_program(), damage=omit_close)
    facts = row['typed_facts']
    assert facts['diagnostic_valid'], facts.get('unknown_reasons')
    sends = [e['event_index'] for e in runtime['events'] if e['event'] == 'send']
    assert len(sends) == 4
    assert [ref['event_index'] for ref in facts['unresolved_historical_obligations']] == sends
    assert row['oracle_verdict'] == 'unknown'
    windows = facts['historical_participant_group_windows']
    assert len(windows) == 1
    assert windows[0]['reader'] == 'r0' and windows[0]['close_ref'] is None
    assert windows[0]['delivery_closure'] == 'unknown'
    assert not windows[0]['window_projection_valid'] and not windows[0]['destroy_operation_refs']


def test_public_runner_current_reader_cannot_waive_retired_reader_closed_gap(tmp_path, transport):
    def omit_receive(events):
        for event in events:
            if (event['event'] == 'receive' and event.get('logical_id') == 'r0'
                    and event.get('send_sequence') == 0):
                event['event'] = 'fixture_omitted_receive'

    row, runtime = run_group_report(tmp_path, mixed_reader_program(), damage=omit_receive)
    facts = row['typed_facts']
    assert facts['diagnostic_valid'], facts.get('unknown_reasons')
    first = next(e for e in runtime['events'] if e['event'] == 'send' and e['send_sequence'] == 0)
    assert [ref['event_index'] for ref in facts['unresolved_historical_obligations']] == [first['event_index']]
    assert row['oracle_verdict'] == 'unknown'
    windows = facts['historical_participant_group_windows']
    assert len(windows) == 1 and windows[0]['window_projection_valid']
    assert windows[0]['delivery_closure'] == 'closed_with_gap'
    detail = windows[0]['proofs']['delivery_closure']['detail']
    assert detail['expected'] == [0, 1, 2, 3]
    assert detail['received'] == [1, 2, 3] and detail['missing'] == [0]


@pytest.mark.parametrize('readers,retired', [(2, 1), (3, 1), (3, 2)])
def test_public_runner_complete_mixed_retired_and_surviving_readers_pass(
        tmp_path, transport, readers, retired):
    program = mixed_reader_program(readers)
    if retired == 2:
        program['actions'].append(action('retire_second_reader', 'destroy', 'r1', 1))
    row, runtime = run_group_report(tmp_path, program)
    facts = row['typed_facts']
    assert facts['diagnostic_valid'], facts.get('unknown_reasons')
    assert row['oracle_verdict'] == 'pass'
    assert facts['unresolved_historical_obligations'] == []
    assert len(facts['historical_participant_group_windows']) == retired
    assert len(facts['historical_live_windows']) == readers - retired
    assert all(w['window_projection_valid'] and w['delivery_closure'] == 'closed'
               for w in facts['historical_participant_group_windows'] + facts['historical_live_windows'])
    # Each send is already classified by a surviving current pair. Verifying its
    # historical counterparts must not label the same send historical as well.
    assert facts['historical_send_coverage'] == []
    assert len([e for e in runtime['events'] if e['event'] == 'send']) == 4
    assert sum(len(d['received_sequences']) for d in runtime['delivery_facts']) == 4 * readers


def test_public_runner_one_verified_retired_reader_cannot_waive_another_missing_close(tmp_path, transport):
    program = mixed_reader_program(3)
    program['actions'].append(action('retire_second_reader', 'destroy', 'r1', 1))

    def omit_second_close(events):
        for event in events:
            if event['event'] == 'topology_delivery_window_closed' and event['reader'] == 'r1':
                event['event'] = 'fixture_omitted_window_close'

    row, runtime = run_group_report(tmp_path, program, damage=omit_second_close)
    facts = row['typed_facts']
    assert facts['diagnostic_valid'], facts.get('unknown_reasons')
    assert row['oracle_verdict'] == 'unknown'
    assert len(facts['unresolved_historical_obligations']) == 4
    windows = {w['reader']: w for w in facts['historical_participant_group_windows']}
    assert set(windows) == {'r0', 'r1'}
    assert windows['r0']['window_projection_valid'] and windows['r0']['delivery_closure'] == 'closed'
    assert not windows['r1']['window_projection_valid'] and windows['r1']['delivery_closure'] == 'unknown'
    assert windows['r1']['close_ref'] is None and not windows['r1']['destroy_operation_refs']


def test_public_runner_verified_close_cannot_waive_overlapping_unclosed_retired_window(tmp_path, transport):
    def add_unclosed_overlap(events):
        opened = next(e for e in events if e['event'] == 'topology_delivery_window_open'
                      and e['reader'] == 'r0')
        # Use a controller-request slot in this authored fixture, preserving all
        # indices and snapshot refs. Actual send/receive evidence stays intact.
        slot = next(e for e in events if e['event'] == 'topology_delivery_window_publish_requested'
                    and e['open_ref'] == opened['event_index'])
        coordinates = {key: slot[key] for key in ('event_index', 'monotonic_ns', 'time_ns')}
        slot.clear()
        slot.update(deepcopy(opened), **coordinates)

    row, runtime = run_group_report(tmp_path, mixed_reader_program(), damage=add_unclosed_overlap)
    facts = row['typed_facts']
    assert facts['diagnostic_valid'], facts.get('unknown_reasons')
    assert row['oracle_verdict'] == 'unknown'
    assert len(facts['unresolved_historical_obligations']) == 4
    windows = facts['historical_participant_group_windows']
    assert len(windows) == 2 and {w['reader'] for w in windows} == {'r0'}
    assert sum(w['window_projection_valid'] and w['delivery_closure'] == 'closed' for w in windows) == 1
    assert sum(w['close_ref'] is None and w['delivery_closure'] == 'unknown' for w in windows) == 1


def test_public_runner_current_forbidden_gap_outranks_retired_reader_missing_close(tmp_path, transport):
    def omit_history_and_current_receive(events):
        for event in events:
            if event['event'] == 'topology_delivery_window_closed' and event['reader'] == 'r0':
                event['event'] = 'fixture_omitted_window_close'
            elif (event['event'] == 'receive' and event.get('logical_id') == 'r1'
                  and event.get('send_sequence') == 0):
                event['event'] = 'fixture_omitted_receive'

    row, runtime = run_group_report(tmp_path, mixed_reader_program(), damage=omit_history_and_current_receive)
    facts = row['typed_facts']
    assert facts['diagnostic_valid'], facts.get('unknown_reasons')
    assert facts['unresolved_historical_obligations']
    assert row['oracle_verdict'] == 'fail'
    assert 'topology.closed_delivery_gap' in row['oracle']['forbidden_residuals']
