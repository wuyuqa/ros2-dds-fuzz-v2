"""Issue #7 checkpoint 1: offline sealing through the existing runner/report API.

Planning fixtures never establish ROS execution, a SUT candidate or maturity.
"""
from concurrent.futures import ThreadPoolExecutor
import json
from pathlib import Path
from threading import Barrier

import pytest

from r2f.core.evidence import canonical_hash, raw_sha256
from r2f.reports.candidate_report import REVIEW_PACKET_SCHEMA, write_review_packet
from r2f.reports.exploratory_builder import persist_f1_exploratory_report
from r2f.reports.run_report import audit_exploratory_report, exploratory_report_digest

from test_f1_exploratory_report import load, plan, registry
from test_feedback_epoch_runner import adapter_runtime


def test_terminal_cycle_references_report_over_a_frozen_input_snapshot(tmp_path):
    cycle, report = plan(tmp_path)
    root = Path(cycle['report_path']).parent
    provenance = report['proof_provenance']
    snapshot_path = root / provenance['cycle_path']
    snapshot = load(snapshot_path)

    # The terminal index points down to the report. Its evidence input must be
    # a separate, already fixed object, never a hash of that future index.
    assert load(root / 'cycle.json') == cycle
    assert snapshot_path != root / 'cycle.json'
    assert canonical_hash(snapshot) == provenance['cycle_sha256']
    assert 'report_path' not in snapshot
    assert 'feedback_aggregate' not in snapshot
    assert snapshot['run_id'] == cycle['run_id'] == report['run_id']
    assert snapshot['source_manifest'] == cycle['source_manifest'] == report['source_manifest']
    assert snapshot['rows'] == cycle['rows']
    assert audit_exploratory_report(cycle['report_path'])['evidence_valid']
    assert report['audit']['sealed'] is False
    assert load(root / 'candidate_ledger.json')['natural_candidate_count'] == 0


@pytest.mark.parametrize('damage', [
    'bytes', 'hash', 'run', 'source', 'cell', 'input', 'target', 'rule',
    'future_index', 'outside_root', 'provenance_shape', 'rows_shape',
])
def test_public_audit_rejects_damaged_or_misbound_cycle_input(tmp_path, damage):
    cycle, report = plan(tmp_path)
    report_path = Path(cycle['report_path'])
    provenance = report['proof_provenance']
    snapshot_path = report_path.parent / provenance['cycle_path']
    snapshot = load(snapshot_path)
    if damage == 'bytes':
        # Even a JSON-equivalent byte change breaks the immutable reference.
        snapshot_path.write_bytes(snapshot_path.read_bytes() + b'\n')
    elif damage == 'hash':
        provenance['cycle_sha256'] = '0' * 64
    elif damage == 'provenance_shape':
        report['proof_provenance'] = 'cycle_path'
    elif damage == 'outside_root':
        outside = tmp_path / 'outside.json'
        outside.write_bytes(snapshot_path.read_bytes())
        provenance['cycle_path'] = '../outside.json'
    else:
        if damage == 'rows_shape':
            snapshot['rows'] = [None]
        elif damage == 'run':
            snapshot['run_id'] = 'other-run'
        elif damage == 'source':
            snapshot['source_manifest']['head'] = 'other-source'
        elif damage == 'future_index':
            snapshot['report_path'] = str(report_path)
        else:
            key = {'cell': 'cell_id', 'input': 'effective_input_sha256',
                   'target': 'target_id', 'rule': 'rule_id'}[damage]
            snapshot['rows'][0][key] = 'other-identity'
        snapshot_path.write_text(json.dumps(snapshot))
        provenance['cycle_sha256'] = canonical_hash(snapshot)
        provenance['cycle_file_sha256'] = raw_sha256(snapshot_path)
    report['report_sha256'] = exploratory_report_digest(report)
    report_path.write_text(json.dumps(report))
    before = {path: path.read_bytes() for path in (report_path, snapshot_path)}
    audited = audit_exploratory_report(report_path)
    assert audited['evidence_valid'] is False, audited
    assert any('cycle_input' in error for error in audited['errors']), audited
    assert {path: path.read_bytes() for path in before} == before


def test_public_audit_rejects_contradictory_source_head_without_changing_originals(tmp_path):
    cycle, report = plan(tmp_path)
    report_path = Path(cycle['report_path'])
    root = report_path.parent
    before = {path: path.read_bytes() for path in root.rglob('*') if path.is_file()}
    assert audit_exploratory_report(report_path)['evidence_valid'] is True

    # Keep both bound source manifests and their snapshot hashes intact. Only
    # the redundant provenance HEAD contradicts that otherwise valid evidence.
    report['proof_provenance']['source_head'] = '0' * 40
    report['report_sha256'] = exploratory_report_digest(report)
    derived_path = root / 'contradictory-source-report.json'
    derived_path.write_text(json.dumps(report))
    audited = audit_exploratory_report(derived_path)
    assert {path: path.read_bytes() for path in before} == before
    assert audited['evidence_valid'] is False, audited
    assert any('source_head' in error for error in audited['errors']), audited


@pytest.mark.parametrize('runtime', [None, adapter_runtime], ids=['planned', 'offline-executed'])
def test_rebuilding_existing_report_preserves_all_original_bytes(tmp_path, runtime):
    cycle, report = plan(tmp_path, runtime=runtime)
    root = Path(cycle['report_path']).parent
    before = {path: path.read_bytes() for path in root.rglob('*') if path.is_file()}
    if runtime is not None:
        assert report['rows'][0]['raw_path']
        assert report['audit']['evidence_valid'] is False
    with pytest.raises(FileExistsError):
        persist_f1_exploratory_report(root, cycle, registry())
    after = {path: path.read_bytes() for path in root.rglob('*') if path.is_file()}
    assert after == before


def test_packet_publication_preserves_an_existing_dangling_symlink(tmp_path):
    path = tmp_path / 'review.json'
    path.symlink_to('missing-original.json')
    packet = {'schema_version': REVIEW_PACKET_SCHEMA, 'packet_id': 'fixture:packet'}
    with pytest.raises(FileExistsError):
        write_review_packet(path, packet)
    assert path.is_symlink()
    assert path.readlink() == Path('missing-original.json')
    assert not (tmp_path / 'missing-original.json').exists()


def test_competing_packet_publishers_leave_one_complete_original(tmp_path):
    path = tmp_path / 'review.json'
    start = Barrier(8)

    def publish(index):
        packet = {'schema_version': REVIEW_PACKET_SCHEMA,
                  'packet_id': 'fixture:' + str(index), 'notes': str(index) * 65536}
        start.wait(timeout=10)
        try:
            write_review_packet(path, packet)
        except FileExistsError:
            return None
        return packet

    with ThreadPoolExecutor(max_workers=8) as pool:
        successes = [packet for packet in pool.map(publish, range(8)) if packet is not None]
    assert len(successes) == 1
    assert load(path) == successes[0]
    assert set(tmp_path.iterdir()) == {path}


@pytest.mark.parametrize('invalid_source', [[], ['invalid'], 'invalid', None, 123, True])
def test_public_audit_reports_malformed_source_without_raising_or_rewriting(tmp_path, invalid_source):
    cycle, report = plan(tmp_path)
    report_path = Path(cycle['report_path'])
    root = report_path.parent
    before = {path: path.read_bytes() for path in root.rglob('*') if path.is_file()}
    snapshot = load(root / report['proof_provenance']['cycle_path'])
    snapshot['source_manifest'] = invalid_source
    derived_snapshot = root / 'malformed-source-input.json'
    derived_snapshot.write_text(json.dumps(snapshot))
    report['source_manifest'] = invalid_source
    report['identity']['source_sha256'] = canonical_hash(invalid_source)
    report['proof_provenance'].update(
        cycle_path=derived_snapshot.name,
        cycle_sha256=canonical_hash(snapshot),
        cycle_file_sha256=raw_sha256(derived_snapshot))
    report['report_sha256'] = exploratory_report_digest(report)
    derived_report = root / 'malformed-source-report.json'
    derived_report.write_text(json.dumps(report))

    audited = audit_exploratory_report(derived_report)
    assert audited['evidence_valid'] is False, audited
    assert any('source_manifest_not_an_object' in error for error in audited['errors']), audited
    assert {path: path.read_bytes() for path in before} == before
