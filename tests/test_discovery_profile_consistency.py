"""Public sidecar intake contracts; vendor bytes are offline fixtures, never DDS runs."""
from pathlib import Path
import time

import pytest

from r2f.campaign.observations import qualify_observation
from test_campaign_observations import capsule_and_overrides, row_for
from test_runtime_instrumentation import PairFixture, GUIDS


def profile_pair(tmp_path, *, mode='instrumentation', spdp=True, transform=lambda raw: raw):
    capsule, inputs = capsule_and_overrides()
    pair = PairFixture(tmp_path, mode=mode,
                       overrides=dict(inputs, observe_adopted_config=mode == 'instrumentation'))
    for event in pair.endpoints.values():
        event['process_start_ticks'] = event['pid'] * 10
    pair.capture()
    now = time.time_ns()
    stamp = f'{now // 10**9}.{now % 10**9:09d}'
    pair.traces()
    for observer, remote in (('writer', 'reader'), ('reader', 'writer')):
        if mode != 'instrumentation':
            continue
        path = Path(pair.ctx.trace_configs[observer].output_file)
        participant = GUIDS[remote][:24] + '000001c1'
        words = ':'.join(participant[i:i+8] for i in range(0, 32, 8))
        lines = (f'{stamp} [174] main: config: Domain/General/Transport/#text: udp {{0}}\n'
                 f'{stamp} [174] main: config: Domain/Discovery/SPDPInterval/#text: 2 s {{0}}\n')
        if spdp:
            lines += f'{stamp} [174] dq.builtin: SPDP ST0 {words} bes fc3f NEW fixture\n'
        path.write_text(transform(lines + path.read_text()))
    for role in ('writer', 'reader'):
        pair.graph(role)
    runtime = pair.finish()
    runtime['observation'] = pair.ctx.collect(runtime)
    return pair, runtime, row_for(pair, runtime), capsule


def test_profile_intake_reports_adoption_announcements_and_pair_completion_separately(tmp_path):
    pair, runtime, row, capsule = profile_pair(tmp_path)
    try:
        result = qualify_observation(row, runtime, pair.c, capsule=capsule)
        assert result['qualification'] == 'qualified', result
        facts = result['discovery_profile']
        assert facts['status'] == 'observed', facts
        assert facts['configuration']['status'] == 'observed'
        assert facts['configuration']['semantic_effect'] == 'unknown'
        assert facts['announcements']['status'] == 'observed'
        assert facts['completion']['status'] == 'observed'
        assert facts['completion']['global_discovery_complete'] is False
        assert facts['candidate_eligible'] is False
        for role, remote in (('writer', 'reader'), ('reader', 'writer')):
            evidence = facts['announcements']['roles'][role]
            assert evidence['participant']['participant_guid'] == GUIDS[remote][:24] + '000001c1'
            assert evidence['endpoint']['endpoint_guid'] == GUIDS[remote]
            assert evidence['remote_endpoint_generation'] == remote + '-generation'
            assert evidence['observer_pid'] == pair.endpoints[role]['pid']
            assert evidence['participant']['raw_sha256']
            assert evidence['endpoint']['raw_line_sha256']
        assert result['admissible_count'] == 0
        assert row['execution_stage'] == 'judged'
    finally:
        pair.close()


@pytest.mark.parametrize('damage', ['missing', 'wrong_peer', 'stale', 'disposed', 'wrong_domain', 'truncated'])
def test_missing_or_invalid_participant_fact_never_becomes_completion_or_candidate(tmp_path, damage):
    def transform(raw):
        lines = raw.splitlines(keepends=True)
        for i, line in enumerate(list(lines)):
            if 'SPDP ST0' not in line:
                continue
            if damage == 'missing':
                lines[i] = ''
            elif damage == 'wrong_peer':
                lines[i] = line.replace('000001c1', '000001c2')
            elif damage == 'stale':
                lines[i] = '1000000000.000000000 ' + line.split(' ', 1)[1]
            elif damage == 'disposed':
                lines[i] = line.replace('SPDP ST0', 'SPDP ST3')
            elif damage == 'wrong_domain':
                lines[i] = line.replace('[174]', '[175]')
            else:
                # A non-terminated SPDP line remains last, not fused to SEDP.
                lines[i] = ''
                lines.append(line.rstrip('\n'))
        return ''.join(lines)

    pair, runtime, row, capsule = profile_pair(tmp_path, transform=transform)
    try:
        result = qualify_observation(row, runtime, pair.c, capsule=capsule)
        assert result['qualification'] == 'qualified', result
        facts = result['discovery_profile']
        assert facts['configuration']['status'] == 'observed'
        assert facts['status'] == facts['announcements']['status'] == facts['completion']['status'] == 'unknown'
        assert facts['reasons']
        assert facts['candidate_eligible'] is False
        assert result['qualified_intake'] == []
    finally:
        pair.close()


def test_blackbox_keeps_profile_and_internal_announcement_claims_unverified(tmp_path):
    pair, runtime, row, capsule = profile_pair(tmp_path, mode='blackbox')
    try:
        result = qualify_observation(row, runtime, pair.c, capsule=capsule)
        assert result['qualification'] == 'blackbox_observed', result
        facts = result['discovery_profile']
        assert facts['status'] == 'unknown'
        assert facts['configuration']['status'] == 'not_run'
        assert facts['announcements']['roles'] == facts['completion']['roles'] == {}
        assert facts['reasons'] == ['internal_discovery_not_observed_in_blackbox']
        assert runtime['observation']['traces'] == []
        assert runtime['observation']['intake'] == []
    finally:
        pair.close()


def test_builtin_dispatch_fact_does_not_assert_datawriter_async_or_a_race(tmp_path):
    pair, runtime, row, capsule = profile_pair(tmp_path)
    try:
        result = qualify_observation(row, runtime, pair.c, capsule=capsule)
        facts = result['discovery_profile']
        asynchronous = facts['asynchronous_discovery']
        assert asynchronous['status'] == 'observed'
        assert asynchronous['fact_kind'] == 'vendor_builtin_dispatch_labels'
        assert asynchronous['user_datawriter_async_status'] == 'unverified'
        assert asynchronous['os_thread_identity_status'] == 'unverified'
        assert asynchronous['race_status'] == 'not_established'
        assert set(asynchronous['roles']) == {'writer', 'reader'}
        knowledge = {case['case_id']: case for case in facts['historical_context']['cases']}
        assert set(knowledge) == {'Fast-DDS-PR6153', 'xrce115', 'eProsima/Micro-XRCE-DDS#41'}
        assert knowledge['Fast-DDS-PR6153']['disposition'] == '后续内部race/TSAN；与6100同一修复链'
        assert knowledge['eProsima/Micro-XRCE-DDS#41']['research_classification'] == '仅测试等待预算调整'
        assert all(case['runtime_applicability']['source_ref'] for case in knowledge.values())
        assert facts['historical_context']['reproduction_status'] == 'not_run'
        assert facts['historical_context']['adds_product_backend'] is False
    finally:
        pair.close()


def test_announcement_on_main_label_does_not_establish_async_dispatch(tmp_path):
    pair, runtime, row, capsule = profile_pair(
        tmp_path, transform=lambda raw: raw.replace('dq.builtin:', 'main:'))
    try:
        result = qualify_observation(row, runtime, pair.c, capsule=capsule)
        facts = result['discovery_profile']
        assert facts['announcements']['status'] == facts['completion']['status'] == 'observed'
        assert facts['asynchronous_discovery']['status'] == 'unknown'
        assert facts['asynchronous_discovery']['roles'] == {}
        assert facts['asynchronous_discovery']['user_datawriter_async_status'] == 'unverified'
    finally:
        pair.close()


def test_rehashed_untrusted_profile_projection_cannot_supply_a_missing_fact(tmp_path):
    from test_campaign_observations import rehash_summary

    pair, runtime, row, capsule = profile_pair(tmp_path, spdp=False)
    try:
        runtime['observation']['discovery_profile'] = {
            'status': 'observed', 'completion': {'status': 'observed'},
            'asynchronous_discovery': {'status': 'observed', 'race_status': 'observed'}}
        rehash_summary(runtime)
        result = qualify_observation(row, runtime, pair.c, capsule=capsule)
        assert result['qualification'] == 'qualified', result
        facts = result['discovery_profile']
        assert facts['completion']['status'] == 'unknown'
        assert facts['asynchronous_discovery']['status'] == 'unknown'
        assert facts['asynchronous_discovery']['race_status'] == 'not_established'
        assert result['admissible_count'] == 0
    finally:
        pair.close()


@pytest.mark.parametrize('retirement', ['gc', 'expired'])
def test_retired_participant_announcement_cannot_close_a_later_graph(tmp_path, retirement):
    def retire(raw):
        lines = raw.splitlines(keepends=True)
        spdp = next(line for line in lines if 'SPDP ST0' in line)
        stamp, rest = spdp.split(' ', 1)
        guid = rest.split('SPDP ST0 ', 1)[1].split(' ', 1)[0]
        if retirement == 'gc':
            body = f'gc_delete_proxy_participant(0x1234, {guid})'
        else:
            body = f'lease expired: l 0x1234 guid {guid} tend 1 < now 2'
        return raw + f'{stamp} [174] gc: {body}\n'

    pair, runtime, row, capsule = profile_pair(tmp_path, transform=retire)
    try:
        result = qualify_observation(row, runtime, pair.c, capsule=capsule)
        assert result['qualification'] == 'qualified', result
        facts = result['discovery_profile']
        assert facts['announcements']['status'] == 'observed'
        assert facts['completion']['status'] == facts['status'] == 'unknown'
        assert 'participant_retired_before_snapshot:writer' in facts['reasons']
        assert result['qualified_intake'] == []
    finally:
        pair.close()


@pytest.mark.parametrize('reannounce', [False, True])
def test_rediscovery_requires_a_new_endpoint_announcement_for_pair_completion(tmp_path, reannounce):
    def rediscover(raw):
        spdp = next(line for line in raw.splitlines(keepends=True) if 'SPDP ST0' in line)
        endpoint = next(line for line in raw.splitlines(keepends=True) if 'SEDP ST0' in line)
        return raw + spdp + (endpoint if reannounce else '')

    pair, runtime, row, capsule = profile_pair(tmp_path, transform=rediscover)
    try:
        result = qualify_observation(row, runtime, pair.c, capsule=capsule)
        assert result['qualification'] == 'qualified', result
        facts = result['discovery_profile']
        assert facts['announcements']['status'] == 'observed'
        assert facts['completion']['status'] == ('observed' if reannounce else 'unknown')
        if not reannounce:
            assert 'endpoint_not_announced_in_current_participant_incarnation:writer' in facts['reasons']
    finally:
        pair.close()
