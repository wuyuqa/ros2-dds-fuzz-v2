"""Unified runner -> immutable artifacts -> report audit, using offline carriers."""
import hashlib
import json
from pathlib import Path
import time

import pytest

from r2f.campaign.runner import run_campaign
from r2f.reports.run_report import audit_report
from test_campaign_evidence import fixture_pair
from test_runtime_instrumentation import PairFixture, GUIDS



class IndexedEvents(list):
    """Give composed fixture events the same contiguous raw indexes as runtime."""

    def append(self, event):
        event['event_index'] = len(self)
        super().append(event)

    def extend(self, events):
        for event in events:
            self.append(event)


def profile_runtime(config, directory, *, participant=True):
    """Compose established primary/sidecar fixture carriers without bypassing gates."""
    pair = PairFixture(Path(directory) / 'sidecar-fixture', overrides=config)
    try:
        primary = fixture_pair(config, Path(directory) / 'primary-fixture', fresh_matches=True)
        wall, monotonic = time.time_ns(), time.monotonic_ns()
        identities = {'writer-gid': GUIDS['writer'] + '0' * 16,
                      'reader-gid': GUIDS['reader'] + '0' * 16,
                      8001: pair.endpoints['writer']['pid'], 8002: pair.endpoints['reader']['pid']}

        def rebase(value, key=''):
            if isinstance(value, dict):
                return {k: rebase(v, k) for k, v in value.items()}
            if isinstance(value, list):
                return [rebase(v, key) for v in value]
            if isinstance(value, int) and key == 'time_ns':
                return wall + (value - 1_700_000_000_000_000_000) // 1000
            if isinstance(value, int) and key.endswith('monotonic_ns'):
                return monotonic + value // 1000
            return identities.get(value, value) if isinstance(value, (int, str)) else value

        primary = rebase(primary)
        pair.events = IndexedEvents(primary['events'][:-1])
        for role in ('writer', 'reader'):
            endpoint = next(e for e in pair.events if e['event'] == 'endpoint_created' and e['role'] == role)
            endpoint.update(pgid=endpoint['pid'], process_start_ticks=endpoint['pid'] * 10,
                            node_name='r2f_' + role)
            pair.endpoints[role] = endpoint
        pair.capture()
        stamp_ns = time.time_ns()
        stamp = f'{stamp_ns // 10**9}.{stamp_ns % 10**9:09d}'
        pair.traces()
        for observer, remote in (('writer', 'reader'), ('reader', 'writer')):
            path = Path(pair.ctx.trace_configs[observer].output_file)
            lines = (f'{stamp} [{config["domain_id"]}] main: config: Domain/General/Transport/#text: udp {{0}}\n'
                     f'{stamp} [{config["domain_id"]}] main: config: Domain/Discovery/SPDPInterval/#text: 2 s {{0}}\n')
            if participant:
                guid = GUIDS[remote][:24] + '000001c1'
                words = ':'.join(guid[i:i+8] for i in range(0, 32, 8))
                lines += f'{stamp} [{config["domain_id"]}] dq.builtin: SPDP ST0 {words} bes fc3f NEW fixture\n'
            path.write_text(lines + path.read_text().replace('rt/r2f_probe', 'rt' + config['topic']))
        for role in ('writer', 'reader'):
            pair.graph(role)
        result = pair.finish()
        result['events'][-1].update(mutation_receipt=primary['mutation_receipt'], real_execution=True)
        raw = Path(result['raw_path'])
        raw.write_text(''.join(json.dumps(e, sort_keys=True) + '\n' for e in result['events']))
        result['raw_sha256'] = hashlib.sha256(raw.read_bytes()).hexdigest()
        result['execution'].update(raw_sha256=result['raw_sha256'],
            source=dict(config['source_manifest'], runtime_source_sha256='b' * 64),
            source_after=dict(config['source_manifest'], runtime_source_sha256='b' * 64),
            workers=[dict(w, pgid=w['pid']) for w in primary['execution_metadata']['workers']])
        for key in ('environment', 'precondition_receipts', 'observation_receipts', 'mutation_receipt'):
            result[key] = primary[key]
        result['execution_metadata'] = result['execution']
        result['evidence_origin'] = 'offline'
        result['observation'] = pair.ctx.collect(result)
        return result
    finally:
        pair.close()


@pytest.mark.parametrize('participant', [True, False])
def test_runner_persists_profile_facts_and_keeps_fixture_out_of_seal(tmp_path, participant):
    report = run_campaign(tmp_path / 'campaign', run_id='offline-profile-report',
        domain_id=174, execute=True, max_executions=1,
        observation={'observation_mode': 'instrumentation', 'observe_adopted_config': True},
        runtime=lambda c, d: profile_runtime(c, d, participant=participant))
    row = report['rows'][0]
    assert row['execution_stage'] == 'judged', (row.get('abort_reason'), row.get('runner_exception'))
    intake = row['instrumentation_observation']
    assert intake['qualification'] == 'qualified', intake['errors']
    facts = intake['discovery_profile']
    assert facts['status'] == ('observed' if participant else 'unknown')
    assert facts['completion']['status'] == ('observed' if participant else 'unknown')
    assert facts['configuration']['status'] == 'observed'
    assert facts['execution_identity']['effective_input_sha256'] == row['effective_input_sha256']
    assert facts['execution_identity']['run_id'] == report['run_id']
    saved = json.loads((tmp_path / 'campaign' / 'report.json').read_text())
    assert saved['rows'][0]['instrumentation_observation'] == intake
    assert report['full_scope_required_cells'] == 72
    assert report['formal_runtime'] is False and report['seal']['sealed'] is False
    original = Path(row['raw_path']).read_bytes()
    assert intake['qualified_intake'] == []
    audited = audit_report(tmp_path / 'campaign' / 'report.json')
    assert any('offline_fixture_cannot_seal' in error for error in audited['errors']), audited
    assert audited['sealed'] is False
    ledger = json.loads(Path(report['candidate_ledger_path']).read_text())
    assert ledger['natural_candidate_count'] == ledger['confirmed_candidates'] == 0
    assert Path(row['raw_path']).read_bytes() == original
