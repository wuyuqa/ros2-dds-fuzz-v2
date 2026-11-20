"""Pure offline raw-consumer regressions; historical raw is never rewritten."""
from copy import deepcopy
import ctypes
import hashlib
import json
from pathlib import Path

import pytest

from r2f.campaign.topology_filter_decision import native_filter_decision_evidence, _decode
from r2f.instrumentation.fastdds_filter_trace.observer import Record, decode_record
from r2f.instrumentation.fastdds_peer.activation import reference_from_config


@pytest.fixture(autouse=True)
def no_native_loader(monkeypatch):
    def forbidden(*args, **kwargs):
        raise AssertionError('offline consumer must not load any native library')
    monkeypatch.setattr(ctypes, 'CDLL', forbidden)


@pytest.fixture
def live_v5():
    base = Path(__file__).resolve().parents[1] / 'results/main-review/s1-fastdds-peer-runtime-20261120/live-v5'
    cases = list(base.glob('original/cases/*/runtime_config.json'))
    if len(cases) != 1:
        pytest.skip('optional immutable live-v5 evidence capsule is not installed')
    config = json.loads(cases[0].read_text())
    path = cases[0].parent / 'runtime/inner-runtime/raw.jsonl'
    rows = [json.loads(line) for line in path.read_text().splitlines()]
    peer = reference_from_config(config)
    if not Path(peer['manifest_path']).exists():
        pytest.skip('live-v5 independently pinned native build artifacts are not installed')
    digest = hashlib.sha256(path.read_bytes()).hexdigest()
    def run(events=rows, *, program=None, reference=peer, before_index=None):
        return native_filter_decision_evidence(program or config['topology_program'], events,
            peer_reference=reference, before_index=before_index,
            ref=lambda e: dict(path=str(path), sha256=digest, event_index=e['event_index']))
    return rows, run


def test_live_v5_two_actual_returns_use_send_sequence_and_preserve_raw(live_v5):
    rows, run = live_v5
    before = deepcopy(rows)
    value = run()
    assert value['declared'] and value['unknowns'] == []
    assert [(o['sequence'], o['decision']) for o in value['observations']] == [(2, False), (3, True)]
    assert [o['send_ref']['event_index'] for o in value['observations']] == [395, 478]
    assert [o['control_ref']['event_index'] for o in value['observations']] == [421, 503]
    for o in value['observations']:
        assert o['reader_incarnation'] == 1 and o['action_id'] == 'reject_original'
        assert o['action_result_ref']['event_index'] == 351
        assert o['state_ref']['event_index'] == 347
        assert o['capture_started_ns'] <= o['capture_finished_ns']
        assert o['filter_effect_observed'] is None and o['candidate_eligible'] is False
    assert rows == before


@pytest.mark.parametrize('fault', ['native_bool', 'native_overflow', 'receipt_bool_alias',
    'control_pubseq', 'mapping_inode', 'health_drop', 'capture_after_next_update', 'missing_selection'])
def test_live_declared_bad_evidence_stays_unknown_not_legacy(live_v5, fault):
    rows, run = live_v5
    row = rows[396]
    if fault == 'native_bool':
        row['receipt']['native_result']['ordinal'] = True
    elif fault == 'native_overflow':
        row['receipt']['native_result']['sequence_low'] += 1 << 32
    elif fault == 'receipt_bool_alias':
        row['receipt']['decision'] = 0
    elif fault == 'control_pubseq':
        rows[421]['message_info_receipt']['fields']['publication_sequence_number']['value'] = 99
    elif fault == 'mapping_inode':
        row['receipt']['provider_mapping']['inode'] += 1
    elif fault == 'health_drop':
        rows[819]['stats'].update(produced=3, dropped=1)
    elif fault == 'missing_selection':
        rows[40]['event'] = 'offline_removed_selection'
    else:
        # Move a real later command's start across capture without reindexing.
        before = next(e for e in rows if e['event'] == 'endpoint_identity_snapshot'
                      and e.get('command_id') == 13 and e['phase'] == 'before')
        before['monotonic_ns'] = row['receipt']['capture_started_monotonic_ns'] - 1
    value = run()
    assert value['declared'] is True and value['unknowns']
    assert not any(o['decision_ref']['event_index'] == 396 for o in value['observations'])
    assert any(u['raw_ref']['event_index'] == 396 for u in value['unknowns'])
    assert all(u['filter_effect_observed'] is None and u['candidate_eligible'] is False
               for u in value['unknowns'])


def test_truncated_capture_cannot_claim_final_health(live_v5):
    _, run = live_v5
    value = run(before_index=500)
    assert value['declared'] and not value['observations'] and value['unknowns']


def test_undeclared_blackbox_is_unchanged():
    assert native_filter_decision_evidence({'actions': []}, [], peer_reference=None,
        ref=lambda row: None) == dict(schema='r2f.topology-native-filter-decision-evidence/v1',
                                     declared=False, observations=[], unknowns=[])


def test_declared_raw_without_explicit_manifest_cannot_disappear(live_v5):
    _, run = live_v5
    value = run(reference=None)
    assert value['declared'] and not value['observations']
    assert {u['raw_ref']['event_index'] for u in value['unknowns']} == {396, 479}


def test_portable_record_decode_rejects_bool_and_overflow_before_ctypes_conversion():
    record = Record(schema_version=1, struct_size=ctypes.sizeof(Record), ordinal=1,
                    started_ns=10, completed_ns=11, pid=12, decision=0, status=0,
                    payload_length=9, payload_copied=9, sequence_high=0, sequence_low=3,
                    encapsulation=1)
    record.reader_guid[:] = bytes.fromhex('01' * 16)
    record.writer_guid[:] = bytes.fromhex('02' * 16)
    record.payload[:9] = b'\0\1\0\0\1\0\0\0\0'
    receipt = decode_record(record, now_ns=20, pid=12)
    receipt.update(manifest_reference={}, provider_mapping={})
    row = dict(monotonic_ns=20, pid=12, receipt=receipt)
    assert _decode(row)['decision'] is False
    for key, bad in [('ordinal', True), ('sequence_low', 1 << 32), ('sequence_high', 1 << 31)]:
        altered = deepcopy(row)
        altered['receipt']['native_result'][key] = bad
        with pytest.raises(ValueError, match='native_integer_type_or_range'):
            _decode(altered)


def test_delayed_envelope_does_not_rebind_capture_to_restored_reader(live_v5):
    rows, run = live_v5
    # Authored drain-delay variation, not an extra real execution. Capture was
    # on incarnation 1; a later envelope alone must not select incarnation 2.
    rows[396]['monotonic_ns'] = rows[580]['monotonic_ns'] + 1
    value = run()
    assert value['unknowns'] == []
    assert [(o['sequence'], o['reader_incarnation']) for o in value['observations']] == [(2, 1), (3, 1)]


def test_reader_actor_capture_can_finish_after_writer_return(live_v5):
    rows, run = live_v5
    # Authored alternate capture-location contract, not evidence of a real
    # reader-path evaluate in live-v5 (whose two actual actors were writers).
    reader = rows[30]
    lineage = ('logical_id', 'generation', 'endpoint_generation', 'pid', 'pgid',
               'process_start_ticks', 'run_id', 'domain_id', 'monotonic_clock_identity',
               'rmw_identifier', 'node_name', 'topic', 'message_type', 'role')
    for index, sendref in ((396, 395), (479, 478)):
        event = rows[index]
        event.update({k: deepcopy(reader[k]) for k in lineage})
        receipt = event['receipt']
        receipt['process_pid'] = receipt['native_result']['pid'] = reader['pid']
        completed = rows[sendref]['publish_occurrence_receipt']['call_return_monotonic_ns'] + 1
        receipt['capture_completed_monotonic_ns'] = receipt['native_result']['completed_ns'] = completed
    for index in (397, 480):
        rows[index].update({k: deepcopy(reader[k]) for k in lineage})
    rows[796]['stats'].update(produced=2, consumed=2, resolve_status=1)
    rows[819]['stats'].update(produced=0, consumed=0, resolve_status=0)
    value = run()
    assert value['unknowns'] == []
    assert [(o['sequence'], o['decision']) for o in value['observations']] == [(2, False), (3, True)]
    assert all(o['filter_effect_observed'] is None for o in value['observations'])
