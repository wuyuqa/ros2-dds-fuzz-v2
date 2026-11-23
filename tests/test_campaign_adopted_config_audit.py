"""Public intake contracts with file-only vendor traces; no live DDS evidence."""
from copy import deepcopy
import json
from pathlib import Path

import pytest

from r2f.campaign.observations import qualify_observation
from test_campaign_observations import capsule_and_overrides, row_for, rehash_ref, rehash_summary
from test_runtime_instrumentation import PairFixture

CONFIG_LINES = (
    '1794670000.123456 [174] main: config: Domain/General/Transport/#text: udp {0}\n'
    '1794670000.123457 [174] main: config: Domain/Discovery/SPDPInterval/#text: 2 s {0,1}\n'
)


@pytest.fixture
def adopted_pair(tmp_path):
    capsule, inputs = capsule_and_overrides()
    pair = PairFixture(tmp_path, overrides=dict(inputs, observe_adopted_config=True))
    try:
        for event in pair.endpoints.values():
            stat = (pair.proc / str(event['pid']) / 'stat').read_text()
            event['process_start_ticks'] = int(stat.rsplit(')', 1)[1].split()[19])
        runtime = pair.ready()
        for trace in pair.ctx.trace_configs.values():
            path = Path(trace.output_file)
            path.write_text(CONFIG_LINES + path.read_text())
        runtime['observation'] = pair.ctx.collect(runtime)
        yield pair, runtime, row_for(pair, runtime), capsule
    finally:
        pair.close()


def qualify(fixture):
    pair, runtime, row, capsule = fixture
    return qualify_observation(row, runtime, pair.c, capsule=capsule)


def test_rehashed_adoption_value_forgery_is_rejected(adopted_pair):
    _, runtime, _, _ = adopted_pair
    runtime['observation']['adopted_domain_settings']['roles']['writer']['fields']['transport']['value'] = 'tcp'
    rehash_summary(runtime)
    result = qualify(adopted_pair)
    assert result['qualification'] == 'not_evaluated', result
    assert any('adopted' in error for error in result['errors'])


def test_presented_configuration_cannot_claim_worker_read(adopted_pair):
    _, runtime, _, _ = adopted_pair
    runtime['observation']['presented_configuration']['writer']['worker_byte_read'] = 'observed'
    rehash_summary(runtime)
    result = qualify(adopted_pair)
    assert result['qualification'] == 'not_evaluated', result
    assert any('presented_configuration' in error for error in result['errors'])


def test_explicit_opt_in_without_adoption_summary_is_not_silently_qualified(adopted_pair):
    _, runtime, _, _ = adopted_pair
    runtime['observation'].pop('adopted_domain_settings')
    rehash_summary(runtime)
    result = qualify(adopted_pair)
    assert result['qualification'] == 'not_evaluated', result
    assert any('missing for explicit' in error for error in result['errors'])


def test_role_cannot_borrow_other_workers_adoption_projection(adopted_pair):
    _, runtime, _, _ = adopted_pair
    roles = runtime['observation']['adopted_domain_settings']['roles']
    roles['reader'] = roles['writer']
    rehash_summary(runtime)
    result = qualify(adopted_pair)
    assert result['qualification'] == 'not_evaluated', result
    assert any('adopted_domain_settings' in error for error in result['errors'])


def test_old_summary_without_adoption_fields_remains_qualified_but_unknown(tmp_path):
    capsule, inputs = capsule_and_overrides()
    pair = PairFixture(tmp_path, overrides=inputs)
    try:
        runtime = pair.ready()
        for trace in pair.ctx.trace_configs.values():
            path = Path(trace.output_file)
            path.write_text(CONFIG_LINES + path.read_text())
        runtime['observation'] = pair.ctx.collect(runtime)
        runtime['observation'].pop('adopted_domain_settings')
        runtime['observation'].pop('presented_configuration')
        rehash_summary(runtime)
        result = qualify_observation(row_for(pair, runtime), runtime, pair.c, capsule=capsule)
        assert result['qualification'] == 'qualified', result
        assert result['adoption_audit_status'] == 'legacy_unverified'
        assert result['semantic_effect'] == 'unknown'
    finally:
        pair.close()


def test_valid_adoption_summary_is_verified_without_claiming_semantic_effect(adopted_pair):
    result = qualify(adopted_pair)
    assert result['qualification'] == 'qualified', result
    assert result['adoption_audit_status'] == 'verified'
    assert result['semantic_effect'] == 'unknown'


def test_semantic_effect_cannot_be_promoted_by_configuration_trace(adopted_pair):
    _, runtime, _, _ = adopted_pair
    runtime['observation']['semantic_effect'] = 'observed'
    rehash_summary(runtime)
    result = qualify(adopted_pair)
    assert result['qualification'] == 'not_evaluated', result
    assert any('semantic_effect' in error for error in result['errors'])


def test_adopted_raw_reference_must_match_recomputed_trace(adopted_pair):
    _, runtime, _, _ = adopted_pair
    field = runtime['observation']['adopted_domain_settings']['roles']['writer']['fields']['transport']
    field['references'][0]['raw_sha256'] = '0' * 64
    rehash_summary(runtime)
    result = qualify(adopted_pair)
    assert result['qualification'] == 'not_evaluated', result
    assert any('adopted_domain_settings' in error for error in result['errors'])


@pytest.mark.parametrize('tamper', ['role', 'pid', 'domain', 'runtime_raw', 'process_ref'])
def test_endpoint_and_process_bindings_are_recomputed(adopted_pair, tamper):
    _, runtime, _, _ = adopted_pair
    binding = runtime['observation']['adopted_domain_settings']['roles']['writer']['endpoint_binding']
    if tamper == 'role':
        binding['role'] = 'reader'
    elif tamper == 'pid':
        binding['pid'] += 1
    elif tamper == 'domain':
        binding['endpoint_event']['domain_id'] += 1
    elif tamper == 'runtime_raw':
        binding['runtime_raw_sha256'] = 'f' * 64
    else:
        binding['process_capture']['raw_sha256'] = 'e' * 64
    rehash_summary(runtime)
    result = qualify(adopted_pair)
    assert result['qualification'] == 'not_evaluated', result
    assert any('adopted_domain_settings' in error for error in result['errors'])


def test_adoption_layers_are_reported_separately(adopted_pair):
    result = qualify(adopted_pair)
    assert result['configuration_presentation_status'] == 'verified'
    assert result['configuration_adoption_status'] == 'observed'
    assert result['semantic_effect'] == 'unknown'


def test_unparsed_adopted_field_is_named_unknown_without_losing_sedp_result(tmp_path):
    capsule, inputs = capsule_and_overrides()
    pair = PairFixture(tmp_path, overrides=dict(inputs, observe_adopted_config=True))
    try:
        for event in pair.endpoints.values():
            stat = (pair.proc / str(event['pid']) / 'stat').read_text()
            event['process_start_ticks'] = int(stat.rsplit(')', 1)[1].split()[19])
        runtime = pair.ready()
        for trace in pair.ctx.trace_configs.values():
            path = Path(trace.output_file)
            path.write_text(CONFIG_LINES.splitlines(keepends=True)[0] + path.read_text())
        runtime['observation'] = pair.ctx.collect(runtime)
        result = qualify_observation(row_for(pair, runtime), runtime, pair.c, capsule=capsule)
        assert result['qualification'] == 'qualified', result
        assert result['configuration_presentation_status'] == 'verified'
        assert result['configuration_adoption_status'] == 'unknown'
        assert result['adopted_domain_settings']['roles']['writer']['fields']['spdp_interval']['status'] == 'unknown'
        assert result['semantic_effect'] == 'unknown'
    finally:
        pair.close()


def save_capture(fixture, role, capture):
    """Refresh the file and all references; stale hashes must not hide contradictions."""
    _, runtime, _, _ = fixture
    observation = runtime['observation']
    ref = observation['process_captures'][role]
    rehash_ref(ref, capture)
    binding = observation['adopted_domain_settings']['roles'][role]['endpoint_binding']
    binding['process_capture'] = deepcopy(ref)
    rehash_summary(runtime)


def test_rehashed_capture_guid_must_match_raw_endpoint(adopted_pair):
    _, runtime, _, _ = adopted_pair
    observation = runtime['observation']
    capture = json.loads(Path(observation['process_captures']['writer']['raw_path']).read_text())
    capture['endpoint_guid'] = 'f' * 32
    observation['adopted_domain_settings']['roles']['writer']['endpoint_binding']['endpoint_guid'] = 'f' * 32
    save_capture(adopted_pair, 'writer', capture)
    result = qualify(adopted_pair)
    assert result['qualification'] == 'not_evaluated', result
    assert any('worker endpoint GUID differs' in error for error in result['errors'])


@pytest.mark.parametrize('role', ['writer', 'reader'])
def test_rehashed_capture_role_must_match_raw_endpoint(adopted_pair, role):
    _, runtime, _, _ = adopted_pair
    capture = json.loads(Path(runtime['observation']['process_captures'][role]['raw_path']).read_text())
    capture['role'] = 'reader' if role == 'writer' else 'writer'
    save_capture(adopted_pair, role, capture)
    result = qualify(adopted_pair)
    assert result['qualification'] == 'not_evaluated', result
    assert any('worker capture role differs' in error for error in result['errors'])


@pytest.mark.parametrize('role', ['writer', 'reader'])
def test_rehashed_capture_process_pid_must_match_worker_and_raw(adopted_pair, role):
    _, runtime, _, _ = adopted_pair
    observation = runtime['observation']
    capture = json.loads(Path(observation['process_captures'][role]['raw_path']).read_text())
    capture['process']['pid'] += 10
    # Keep the redundant trace/process projections current, so rejection must
    # come from the cross-source PID contradiction rather than a stale summary.
    for trace in observation['traces']:
        if trace['raw_path'] == observation['adopted_domain_settings']['roles'][role]['raw_path']:
            trace['processes'][0]['pid'] = capture['process']['pid']
            trace['adopted_domain_settings']['binding']['pid'] = capture['process']['pid']
            for event in trace['events']:
                event['observer_pid'] = capture['process']['pid']
    observation['adopted_domain_settings']['roles'][role]['binding']['pid'] = capture['process']['pid']
    save_capture(adopted_pair, role, capture)
    result = qualify(adopted_pair)
    assert result['qualification'] == 'not_evaluated', result
    assert any('worker process PID differs' in error for error in result['errors']), result['errors']


@pytest.mark.parametrize('role', ['writer', 'reader'])
@pytest.mark.parametrize('identity_part', ['device', 'inode', 'missing', 'other_role'])
def test_rehashed_capture_audit_must_identify_actual_trace(adopted_pair, role, identity_part):
    _, runtime, _, _ = adopted_pair
    observation = runtime['observation']
    capture = json.loads(Path(observation['process_captures'][role]['raw_path']).read_text())
    if identity_part == 'missing':
        capture['audit'].pop('trace_file_identity')
    elif identity_part == 'other_role':
        other_role = 'reader' if role == 'writer' else 'writer'
        other = json.loads(Path(observation['process_captures'][other_role]['raw_path']).read_text())
        capture['audit']['trace_file_identity'] = other['audit']['trace_file_identity']
    else:
        capture['audit']['trace_file_identity'][0 if identity_part == 'device' else 1] += 1
    save_capture(adopted_pair, role, capture)
    result = qualify(adopted_pair)
    assert result['qualification'] == 'not_evaluated', result
    assert any('trace file identity differs from process audit' in error for error in result['errors'])


@pytest.mark.parametrize('role', ['writer', 'reader'])
@pytest.mark.parametrize('identity_part', ['endpoint_generation', 'process_start_ticks'])
def test_rehashed_capture_generation_must_match_raw_lifetime(adopted_pair, role, identity_part):
    _, runtime, _, _ = adopted_pair
    observation = runtime['observation']
    capture = json.loads(Path(observation['process_captures'][role]['raw_path']).read_text())
    if identity_part == 'endpoint_generation':
        capture['endpoint_event']['endpoint_generation'] = 'other-generation'
        binding = observation['adopted_domain_settings']['roles'][role]['endpoint_binding']
        binding['endpoint_generation'] = 'other-generation'
        binding['endpoint_event'] = deepcopy(capture['endpoint_event'])
        expected_error = 'worker endpoint not bound to raw'
    else:
        ticks = str(int(capture['process']['process_start_ticks']) + 1)
        capture['process']['process_start_ticks'] = ticks
        for trace in observation['traces']:
            if trace['raw_path'] == observation['adopted_domain_settings']['roles'][role]['raw_path']:
                trace['processes'][0]['process_start_ticks'] = ticks
                trace['adopted_domain_settings']['binding']['process_start_ticks'] = ticks
        observation['adopted_domain_settings']['roles'][role]['binding']['process_start_ticks'] = ticks
        expected_error = 'process audit generation differs'
    save_capture(adopted_pair, role, capture)
    result = qualify(adopted_pair)
    assert result['qualification'] == 'not_evaluated', result
    assert any(expected_error in error for error in result['errors']), result['errors']


def test_adoption_without_raw_process_lifetime_cannot_claim_verified_binding(tmp_path):
    capsule, inputs = capsule_and_overrides()
    pair = PairFixture(tmp_path, overrides=dict(inputs, observe_adopted_config=True))
    try:
        runtime = pair.ready()
        for trace in pair.ctx.trace_configs.values():
            path = Path(trace.output_file)
            path.write_text(CONFIG_LINES + path.read_text())
        runtime['observation'] = pair.ctx.collect(runtime)
        result = qualify_observation(row_for(pair, runtime), runtime, pair.c, capsule=capsule)
        assert result['qualification'] == 'not_evaluated', result
        assert any('adopted process lifetime not bound to raw' in error for error in result['errors'])
    finally:
        pair.close()
