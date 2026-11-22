"""Offline CP1 checks at the canonical namespace activity/capture boundary."""
import ast
import json
from copy import deepcopy
from pathlib import Path
from types import SimpleNamespace

import pytest

from r2f.runtime import namespace_pair as ns
from r2f.runtime.ros2_pair import normalize_config

WRITER = '0102030405060708090a0b0c'
READER = '1112131415161718191a1b1c'
SINCE = 1_790_000_000_000_000_000


def config(**overrides):
    return ns.normalize_discovery_config(normalize_config({
        'target_id': 'discovery.interruption_recovery', 'carrier': 'wire_proxy',
        'mutation_kind': 'discovery_drop', **overrides}))


def activity_seam(observer, c=None, endpoints=None, emit_error=None, production_emit=False):
    # Exercise the actual canonical closure without entering its native body.
    tree = ast.parse(Path(ns.__file__).read_text())
    body = next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == '_body')
    activity = next(n for n in body.body if isinstance(n, ast.FunctionDef) and n.name == 'activity')
    emitted = []
    def emit(event, **fields):
        if emit_error is not None:
            raise emit_error
        token = {'event': event, 'raw_event_ref': f'offline#event={len(emitted)}', **fields}
        json.dumps(token, sort_keys=True, allow_nan=False)
        emitted.append(token)
        return token
    scope = {**vars(ns), 'observer': observer, 'c': c if c is not None else config(),
             'endpoints': endpoints if endpoints is not None else {
                 'writer': {'endpoint_gid': WRITER+'000003c2'},
                 'reader': {'endpoint_gid': READER+'000004c7'}}, 'emit': emit}
    nodes = [activity]
    if production_emit:
        nodes.insert(0, next(n for n in body.body if isinstance(n, ast.FunctionDef) and n.name == 'emit'))
        scope.update(events=emitted, raw_path=Path('/offline-fixture/raw.jsonl'),
                     c={**scope['c'], 'run_id': 'offline-lease-boundary', 'domain_id': 141})
    exec(compile(ast.Module(body=nodes, type_ignores=[]), ns.__file__, 'exec'), scope)
    return scope['activity'], emitted


class Observer:
    def __init__(self, lease):
        self.lease = lease
        self.calls = []
    def snapshot_activity(self, **kwargs):
        self.calls.append(('activity', kwargs))
        return {'until_ns': SINCE+100, 'status': 'unknown', 'reason': 'offline_activity'}
    def snapshot_participant_leases(self, **kwargs):
        self.calls.append(('lease', kwargs))
        return deepcopy(self.lease)


def test_opt_in_emits_separate_supporting_lease_receipt_from_same_observer():
    lease = observed_snapshot()
    observer = Observer(lease)
    activity, events = activity_seam(observer, config(observe_participant_leases=True))
    result = activity(SINCE, 'post_restore_0', after_monotonic_ns=77)
    assert result == events[0]
    assert result['event'] == 'discovery_activity_observation'
    assert result['activity_observed'] is False
    assert len(events) == 2
    supporting = events[1]
    assert supporting['event'] == 'participant_lease_observation'
    assert supporting['participant_leases'] == lease
    assert supporting['activity_event_ref'] == result['raw_event_ref']
    assert supporting['window'] == 'post_restore_0'
    assert supporting['receipt_kind'] == 'supporting'
    assert supporting['status'] == 'observed'
    assert supporting['observation_only'] is True
    assert supporting['candidate_eligible'] is False
    assert supporting['no_remote_expiry_proof'] is True
    assert supporting['lifecycle_generation_missing'] is True
    assert supporting['ownership_status'] == 'unknown_separate_ownership_evidence_required'
    assert observer.calls == [
        ('activity', {'since_ns': SINCE, 'participant_prefixes': [WRITER, READER]}),
        ('lease', {'since_ns': SINCE, 'participant_prefixes': [WRITER, READER], 'field_policy': None})]


@pytest.mark.parametrize('value', [None, 0, 1, 'true', 'false', [], {}])
def test_opt_in_rejects_non_boolean_config_before_runtime(value):
    with pytest.raises(ValueError, match='invalid_observe_participant_leases'):
        config(observe_participant_leases=value)


@pytest.mark.parametrize('value', [True, False])
def test_opt_in_accepts_only_explicit_booleans(value):
    assert config(observe_participant_leases=value)['observe_participant_leases'] is value


def test_default_config_disables_lease_observation():
    assert config()['observe_participant_leases'] is False


@pytest.mark.parametrize('failure', ['missing_api', 'unavailable_api', 'runtime_error', 'io_error'])
def test_unavailable_observation_is_named_unknown_and_keeps_activity(failure):
    observer = Observer(None)
    def broken(**kwargs):
        if failure == 'runtime_error':
            raise RuntimeError('bounded capture failed')
        raise OSError('capture not readable')
    observer.snapshot_participant_leases = broken
    if failure == 'missing_api':
        observer = SimpleNamespace(snapshot_activity=observer.snapshot_activity)
    elif failure == 'unavailable_api':
        observer.snapshot_participant_leases = None
    activity, events = activity_seam(observer, config(observe_participant_leases=True))
    result = activity(SINCE, 'discovery_interruption')
    assert result == events[0]
    receipt = events[1]
    assert receipt['status'] == 'unknown'
    assert receipt['participant_leases']['status'] == 'unknown'
    expected = ('participant_lease_snapshot_api_unavailable' if failure.endswith('api')
                else 'participant_lease_snapshot_error')
    assert receipt['participant_leases']['reasons'] == [expected]
    assert receipt['candidate_eligible'] is False


@pytest.mark.parametrize('reason,details', [
    ('missing_requested_participant_lease_observation', {'missing_participant_prefixes': [READER]}),
    ('invalid_parameter_length_or_truncation', {'diagnostics': [{'pcap_offset': 123, 'truncated': True}]}),
    ('activity_window_not_retained', {'window_buffer_retained': False}),
    ('legacy_duration_unknown', {'observations': [{'status': 'unknown', 'lease': None}]}),
])
def test_unknown_missing_participant_and_truncated_details_are_preserved(reason, details):
    lease = {'status': 'unknown', 'reasons': [reason], 'observations': [], **details}
    original = deepcopy(lease)
    activity, events = activity_seam(Observer(lease), config(observe_participant_leases=True))
    activity(SINCE, 'discovery_interruption')
    assert events[1]['participant_leases'] == original
    assert events[1]['status'] == 'unknown'
    assert lease == original
    assert 'mutation_receipt' not in events[1]


@pytest.mark.parametrize('c', [{}, {'observe_participant_leases': False}])
def test_disabled_path_never_calls_lease_api_or_adds_events(c):
    class ActivityOnly:
        def snapshot_activity(self, **kwargs):
            return {'status': 'unknown', 'reason': 'unchanged_activity'}
        def __getattribute__(self, name):
            if name == 'snapshot_participant_leases':
                raise AssertionError('disabled path touched lease API')
            return object.__getattribute__(self, name)
    activity, events = activity_seam(ActivityOnly(), c)
    result = activity(SINCE, 'post_restore', after_monotonic_ns=77)
    assert events == [{
        'event': 'discovery_activity_observation', 'raw_event_ref': 'offline#event=0',
        'window': 'post_restore', 'activity': {'status': 'unknown', 'reason': 'unchanged_activity'},
        'activity_observed': False, 'after_monotonic_ns': 77, 'clock_guard_ns': 50_000_000}]
    assert result is events[0]


def test_original_activity_error_propagates_without_supporting_success():
    observer = Observer({'status': 'observed'})
    error = RuntimeError('original activity failed')
    def broken_activity(**kwargs):
        raise error
    observer.snapshot_activity = broken_activity
    activity, events = activity_seam(observer, config(observe_participant_leases=True))
    with pytest.raises(RuntimeError) as raised:
        activity(SINCE, 'discovery_interruption')
    assert raised.value is error
    assert events == []
    assert observer.calls == []


def test_original_emission_failure_propagates():
    error = OSError('original raw output failed')
    observer = Observer({'status': 'observed'})
    activity, events = activity_seam(observer, config(observe_participant_leases=True), emit_error=error)
    with pytest.raises(OSError) as raised:
        activity(SINCE, 'discovery_interruption')
    assert raised.value is error
    assert events == []
    assert [call[0] for call in observer.calls] == ['activity']


def test_missing_endpoint_identity_is_not_filled_from_config():
    observer = Observer({'status': 'observed'})
    activity, events = activity_seam(observer, config(observe_participant_leases=True), endpoints={})
    with pytest.raises(KeyError):
        activity(SINCE, 'discovery_interruption')
    assert events == []
    assert observer.calls == []


def test_observer_error_details_are_bounded():
    observer = Observer(None)
    def broken(**kwargs):
        raise RuntimeError('x'*2000)
    observer.snapshot_participant_leases = broken
    activity, events = activity_seam(observer, config(observe_participant_leases=True))
    activity(SINCE, 'discovery_interruption')
    assert events[1]['participant_leases']['error_type'] == 'RuntimeError'
    assert len(events[1]['participant_leases']['error']) == 512


@pytest.mark.parametrize('value', [None, {}, [], {'status': 'success'}, {'status': True}])
def test_invalid_optional_api_result_is_named_unknown(value):
    activity, events = activity_seam(Observer(value), config(observe_participant_leases=True))
    result = activity(SINCE, 'discovery_interruption')
    assert result is events[0]
    assert events[1]['status'] == 'unknown'
    assert events[1]['participant_leases']['reasons'] == ['participant_lease_snapshot_invalid_result']


@pytest.mark.parametrize('value', [
    {'status': 'observed'},
    {'status': 'observed', 'observations': [], 'reasons': [], 'raw': None},
    {'status': 'observed', 'observations': 'not-a-list', 'reasons': []},
    {'status': 'observed', 'observations': [],
     'reasons': ['missing_requested_participant_lease_observation']},
    {'status': 'unknown'},
    {'status': 'unknown', 'reasons': []},
    {'status': 'unknown', 'reasons': 'missing_capture'},
    {'status': 'unknown', 'reasons': [None]},
    {'status': 'unknown', 'reasons': ['']},
    {'status': 'unknown', 'reasons': ['missing_capture'], 'observations': 'invalid'},
])
def test_malformed_snapshot_schema_cannot_claim_an_observation(value):
    activity, events = activity_seam(Observer(value), config(observe_participant_leases=True))
    result = activity(SINCE, 'post_restore_0', after_monotonic_ns=77)
    assert result is events[0]
    assert events[0]['activity_observed'] is False
    assert events[1]['participant_leases'] == {
        'status': 'unknown', 'reasons': ['participant_lease_snapshot_invalid_result']}
    assert events[1]['status'] == 'unknown'


def observed_snapshot():
    return {'status': 'observed', 'reasons': [],
            'raw': {'path': 'offline-only.pcap', 'sha256': 'a'*64},
            'observations': [
                {'status': 'observed', 'reasons': [],
                 'participant_guid': prefix+'000001c1',
                 'source_guid_prefix': prefix,
                 'raw_ref': {'pcap_offset': 24},
                 'lease': {'status': 'observed', 'nanoseconds': 3_500_000_000}}
                for prefix in (WRITER, READER)]}


@pytest.mark.parametrize('change', [
    {'raw': {}}, {'raw': {'path': '', 'sha256': 'a'*64}},
    {'raw': {'path': 'offline.pcap', 'sha256': 'not-a-hash'}},
    {'reasons': ['missing_requested_participant_lease_observation']},
    {'missing_participant_prefixes': [READER]}, {'window_buffer_retained': False},
    {'observations': [{}]},
    {'observations': [{'status': 'unknown', 'reasons': ['truncated']}]},
    {'observations': [dict(observed_snapshot()['observations'][0], lease=None)]},
    {'observations': [dict(observed_snapshot()['observations'][0],
                           lease={'status': 'unknown', 'reason': 'legacy'})]},
    {'observations': [dict(observed_snapshot()['observations'][0], raw_ref=None)]},
    {'observations': [dict(observed_snapshot()['observations'][0], participant_guid='')]},
    {'observations': [dict(observed_snapshot()['observations'][0], reasons=['truncated'])]},
])
def test_observed_snapshot_requires_raw_identity_and_consistent_observations(change):
    value = {**observed_snapshot(), **change}
    activity, events = activity_seam(Observer(value), config(observe_participant_leases=True))
    assert activity(SINCE, 'post_restore_0') is events[0]
    assert events[1]['participant_leases'] == {
        'status': 'unknown', 'reasons': ['participant_lease_snapshot_invalid_result']}


def test_valid_observed_snapshot_is_preserved():
    value = observed_snapshot()
    activity, events = activity_seam(Observer(value), config(observe_participant_leases=True))
    activity(SINCE, 'post_restore_0')
    assert events[1]['status'] == 'observed'
    assert events[1]['participant_leases'] == value


@pytest.mark.parametrize('bad', [{1, 2}, b'bytes', object(), float('nan'), float('inf'),
                                {1: 'mixed', 'key': 'keys'}])
@pytest.mark.parametrize('status', ['observed', 'unknown'])
def test_non_json_optional_payload_is_bounded_unknown_before_emit(bad, status):
    value = (observed_snapshot() if status == 'observed'
             else {'status': 'unknown', 'reasons': ['capture_missing']})
    value['diagnostic'] = bad
    activity, events = activity_seam(Observer(value), config(observe_participant_leases=True))
    assert activity(SINCE, 'post_restore_0') is events[0]
    assert events[1]['participant_leases'] == {
        'status': 'unknown', 'reasons': ['participant_lease_snapshot_invalid_result']}
    json.dumps(events, sort_keys=True, allow_nan=False)


def test_circular_optional_payload_is_bounded_unknown_before_emit():
    value = {'status': 'unknown', 'reasons': ['capture_missing']}
    value['diagnostic'] = value
    activity, events = activity_seam(Observer(value), config(observe_participant_leases=True))
    assert activity(SINCE, 'post_restore_0') is events[0]
    assert events[1]['participant_leases'] == {
        'status': 'unknown', 'reasons': ['participant_lease_snapshot_invalid_result']}


def test_supporting_raw_emission_failure_propagates_after_validated_snapshot():
    error = OSError('supporting raw write failed')
    activity, events = activity_seam(Observer(observed_snapshot()), config(observe_participant_leases=True))
    original = activity.__globals__['emit']
    def broken_emit(event, **fields):
        if event == 'participant_lease_observation':
            raise error
        return original(event, **fields)
    activity.__globals__['emit'] = broken_emit
    with pytest.raises(OSError) as raised:
        activity(SINCE, 'post_restore_0')
    assert raised.value is error
    assert len(events) == 1


@pytest.mark.parametrize('change', [
    {'observations': observed_snapshot()['observations'][:1]},
    {'observations': [dict(item, source_guid_prefix='ff'*12)
                      for item in observed_snapshot()['observations']]},
    {'observations': [dict(item, lease={'status': 'observed'})
                      for item in observed_snapshot()['observations']]},
])
def test_observed_requires_requested_pair_and_lease_value(change):
    activity, events = activity_seam(Observer({**observed_snapshot(), **change}),
                                     config(observe_participant_leases=True))
    assert activity(SINCE, 'post_restore_0') is events[0]
    assert events[1]['participant_leases'] == {
        'status': 'unknown', 'reasons': ['participant_lease_snapshot_invalid_result']}


@pytest.mark.parametrize('lease', [
    {'status': 'observed', 'infinite': True, 'nanoseconds': None},
    {'status': 'observed', 'infinite': False, 'nanoseconds': None,
     'exact_seconds_numerator': 1, 'exact_seconds_denominator': 4294967296},
])
def test_valid_infinite_and_exact_fraction_advertisements_remain_observed(lease):
    value = observed_snapshot()
    for item in value['observations']:
        item['lease'] = lease
    activity, events = activity_seam(Observer(value), config(observe_participant_leases=True))
    activity(SINCE, 'post_restore_0')
    assert events[1]['participant_leases'] == value
    assert events[1]['status'] == 'observed'


@pytest.mark.parametrize('fields', [
    {'nanoseconds': -1, 'exact_seconds_numerator': 1, 'exact_seconds_denominator': 2},
    {'infinite': True, 'nanoseconds': -1},
    {'nanoseconds': True, 'exact_seconds_numerator': 1, 'exact_seconds_denominator': 2},
    {'infinite': 'false'}, {'infinite': None}, {'nanoseconds': 3.5},
    {'exact_seconds_numerator': -1, 'exact_seconds_denominator': 2},
    {'exact_seconds_numerator': True, 'exact_seconds_denominator': 2},
    {'exact_seconds_numerator': 7, 'exact_seconds_denominator': False},
    {'exact_seconds_numerator': 7}, {'exact_seconds_denominator': 2},
    {'exact_seconds_numerator': 1, 'exact_seconds_denominator': 0},
    {'exact_seconds_numerator': 1, 'exact_seconds_denominator': 2},
    {'infinite': True, 'nanoseconds': 0},
    {'infinite': True, 'nanoseconds': None,
     'exact_seconds_numerator': 1, 'exact_seconds_denominator': 2},
    {'seconds': -1, 'second_word': 0},
    {'seconds': True, 'second_word': 0},
    {'seconds': 3, 'second_word': 4294967296},
    {'seconds': 3}, {'second_word': 0},
    {'seconds': 3, 'second_word': 0},
    {'nanoseconds': 2147483648000000001},
    {'nanoseconds': None, 'exact_seconds_numerator': 2147483648,
     'exact_seconds_denominator': 1},
    {'nanoseconds_policy': 'invented'},
])
def test_present_duration_fields_cannot_contradict_other_representations(fields):
    value = observed_snapshot()
    value['observations'][0]['lease'].update(fields)
    activity, events = activity_seam(Observer(value), config(observe_participant_leases=True))
    assert activity(SINCE, 'offline_duration') is events[0]
    assert events[1]['participant_leases'] == {
        'status': 'unknown', 'reasons': ['participant_lease_snapshot_invalid_result']}


@pytest.mark.parametrize('seconds,word', [(0, 0), (0, 1), (0, 0x80000000),
                                        (3, 0xffffffff), (0x7fffffff, 0xfffffffe),
                                        (0x7fffffff, 0xffffffff)])
@pytest.mark.parametrize('profile', ['standard', 'integral_legacy', 'nearest_ns', 'floor_ns'])
def test_real_duration_parser_values_preserved(tmp_path, seconds, word, profile):
    import hashlib
    import struct
    from r2f.instrumentation.sedp_fields import LEGACY, decode_duration

    vendor, version, policy = '0110', (2, 5), None
    if profile == 'integral_legacy':
        version = (2, 1)
    elif profile in ('nearest_ns', 'floor_ns'):
        vendor, version, release = next(key for key, value in LEGACY.items()
                                         if value['rounding'] == profile)
        # Offline hashed provenance exercises the parser's actual legacy policy.
        manifest = tmp_path / 'offline-profile.json'
        data = json.dumps({'library_sha256': LEGACY[(vendor, version, release)]['library_sha256']}).encode()
        manifest.write_bytes(data)
        policy = {'vendor_versions': {vendor: release}, 'vendor_provenance': {
            vendor: {'path': str(manifest), 'sha256': hashlib.sha256(data).hexdigest()}}}
    lease = decode_duration(struct.pack('<iI', seconds, word), '<', vendor_id=vendor,
                            protocol_version=version, field_policy=policy)
    if lease['status'] == 'unknown':
        # Unverified legacy fractions remain unknown at the producer boundary.
        assert profile == 'integral_legacy' and word != 0
        return
    value = observed_snapshot()
    for item in value['observations']:
        item['lease'] = lease
    activity, events = activity_seam(Observer(value), config(observe_participant_leases=True))
    activity(SINCE, 'offline_parser')
    assert events[1]['participant_leases'] == value
    assert events[1]['candidate_eligible'] is False


def test_exact_rational_without_optional_nanoseconds_is_preserved():
    value = observed_snapshot()
    for item in value['observations']:
        item['lease'] = {'status': 'observed', 'exact_seconds_numerator': 7,
                         'exact_seconds_denominator': 2}
    activity, events = activity_seam(Observer(value), config(observe_participant_leases=True))
    activity(SINCE, 'offline_rational')
    assert events[1]['participant_leases'] == value


@pytest.mark.parametrize('format_error', [ValueError, RecursionError, KeyboardInterrupt])
def test_unformattable_optional_exception_still_returns_canonical_activity(format_error):
    class BrokenDetail(Exception):
        def __str__(self):
            raise format_error('offline broken formatter')
    class BrokenObserver(Observer):
        def snapshot_participant_leases(self, **kwargs):
            raise BrokenDetail()
    activity, events = activity_seam(BrokenObserver(None), config(observe_participant_leases=True))
    assert activity(SINCE, 'offline_error') is events[0]
    detail = events[1]['participant_leases']
    assert detail['status'] == 'unknown'
    assert detail['reasons'] == ['participant_lease_snapshot_error']
    assert type(detail['error']) is str and len(detail['error']) <= 512
    assert type(detail['error_type']) is str and len(detail['error_type']) <= 128
    assert events[1]['candidate_eligible'] is False


@pytest.mark.parametrize('shape', ['deep_dict', 'deep_list', 'wide', 'non_string_key'])
def test_optional_shape_is_bounded_before_production_envelope_serialization(shape):
    value = {'status': 'unknown', 'reasons': ['offline_capture_missing']}
    diagnostic = None
    if shape.startswith('deep'):
        for _ in range(64):
            diagnostic = {'child': diagnostic} if shape == 'deep_dict' else [diagnostic]
    elif shape == 'wide':
        diagnostic = [None] * 100001
    else:
        diagnostic = {1: 'JSON would silently coerce this key'}
    value['diagnostic'] = diagnostic
    observer = SimpleNamespace(snapshot_activity=lambda **kw: {'status': 'unknown'},
                               snapshot_participant_leases=lambda **kw: value)
    activity, events = activity_seam(observer, config(observe_participant_leases=True),
                                     production_emit=True)
    assert activity(SINCE, 'offline_shape') is events[0]
    assert events[1]['participant_leases'] == {
        'status': 'unknown', 'reasons': ['participant_lease_snapshot_invalid_result']}
    assert events[1]['activity_event_ref'] == events[0]['raw_event_ref']
    assert events[1]['candidate_eligible'] is False


def test_bounded_optional_diagnostic_survives_production_envelope():
    value = {'status': 'unknown', 'reasons': ['offline_capture_missing']}
    diagnostic = {'detail': ['offline', 1, None, True]}
    for _ in range(12):
        diagnostic = {'child': diagnostic}
    value['diagnostic'] = diagnostic
    activity, events = activity_seam(Observer(value), config(observe_participant_leases=True),
                                     production_emit=True)
    assert activity(SINCE, 'offline_shape') is events[0]
    assert events[1]['participant_leases'] == value
