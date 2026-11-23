"""Offline public profile/native-provider seam; no DDS library or ROS context."""
import hashlib
import json
import os
from pathlib import Path

import pytest

from r2f.runtime.discovery_profiles import CYCLONE_XML, write_profiles


def test_opt_in_enables_real_xml_for_pair_and_observer_only(tmp_path):
    legacy = tmp_path / 'legacy'
    legacy.mkdir()
    records, _ = write_profiles(legacy, 'rmw_cyclonedds_cpp')
    assert Path(records['writer']['path']).read_text() == CYCLONE_XML
    records, environments = write_profiles(
        tmp_path, 'rmw_cyclonedds_cpp', discovery_layers='participant_topic_v1')
    assert set(records) == {'writer', 'reader', 'builtin_observer'}
    for role, record in records.items():
        data = Path(record['path']).read_bytes()
        assert b'<Discovery><EnableTopicDiscoveryEndpoints>true</EnableTopicDiscoveryEndpoints>' in data
        assert record['sha256'] == hashlib.sha256(data).hexdigest()
        assert environments[role] == {'CYCLONEDDS_URI': record['path']}
    with pytest.raises(ValueError, match='unsupported'):
        write_profiles(tmp_path / 'unsupported', 'rmw_fastrtps_cpp', discovery_layers='participant_topic_v1')


class NativeProvider:
    """External native API stand-in; owns loan receipts and close semantics."""
    def __init__(self, fail=None):
        self.fail = fail
        self.closed = False
        self.counts = {}

    def open(self, config):
        assert config['live_authorization']['granted'] is True
        assert Path(os.environ['CYCLONEDDS_URI']).read_text().count(
            '<EnableTopicDiscoveryEndpoints>true</EnableTopicDiscoveryEndpoints>') == 1
        return {'receipt_kind': 'native_participant_created', 'guid': 'aa'*12+'000001c1',
                'participant_handle': 10, 'domain_id': config['domain_id'], 'real_execution': True}

    def builtin_reader(self, topic):
        if self.fail == topic:
            raise RuntimeError('reader_failed')
        return {'receipt_kind': 'native_builtin_reader_created', 'real_execution': True, 'reader_handle':
                11 if topic == 'DCPSParticipant' else 12, 'topic': topic,
                'topic_handle': 0x7fff0001 if topic == 'DCPSParticipant' else 0x7fff0002}

    def take_participants(self, reader):
        return self.take(reader, {'guid': '01'*12+'000001c1', 'valid_data': True,
                                  'instance_state': 'ALIVE'})

    def take_topics(self, reader):
        return self.take(reader, {'topic_key': 'ab'*16, 'key_kind': 'opaque16',
            'topic_name': 'demo', 'type_name': 'Msg', 'valid_data': True,
            'history': {'status': 'observed', 'kind': 'KEEP_LAST', 'depth': 7},
            'instance_state': 'ALIVE'})

    def take(self, reader, sample):
        self.counts[reader] = self.counts.get(reader, 0) + 1
        samples = [sample] if self.counts[reader] == 1 else []
        return {'receipt_kind': 'native_builtin_participants' if reader == 11 else 'native_builtin_topics',
                'ok': True, 'accepted': True, 'real_execution': True, 'reader_handle': reader,
                'samples': samples, 'take_return_code': len(samples),
                'loan_cleanup_complete': True, 'loan_returned': bool(samples)}

    def close(self):
        self.closed = True
        return {'receipt_kind': 'native_participant_closed', 'cleanup_complete': True, 'real_execution': True}


def observe(tmp_path, provider=None):
    from r2f.runtime.discovery_builtin_observer import DiscoveryBuiltinObserver
    profiles, envs = write_profiles(tmp_path, 'rmw_cyclonedds_cpp',
                                   discovery_layers='participant_topic_v1')
    observer = DiscoveryBuiltinObserver(tmp_path / 'builtin.json', provider_factory=lambda: provider or NativeProvider())
    observer.start(domain_id=19, admission={'event': 'namespace_host_admission_verified',
        'raw_event_ref': 'raw.jsonl#namespace_event=0', 'domain_id': 19, 'pid': os.getpid()}, profile=profiles['builtin_observer'],
        environment=envs['builtin_observer'])
    return observer


def test_observer_retains_takes_cleanup_and_hash_backed_samples(tmp_path):
    provider = NativeProvider()
    observer = observe(tmp_path, provider)
    receipt = observer.finish()
    assert provider.closed
    data = Path(receipt['raw']['path']).read_bytes()
    assert hashlib.sha256(data).hexdigest() == receipt['raw']['sha256']
    raw = json.loads(data)
    assert raw['status'] == 'observed'
    assert raw['samples']['DCPSTopic'][0]['topic_key'] == 'ab'*16
    assert raw['samples']['DCPSParticipant'][0]['guid'] == '01'*12+'000001c1'
    assert raw['cleanup_complete'] is True
    assert raw['started_unix_ns'] <= raw['closed_unix_ns']
    assert receipt == observer.finish()  # no rewrite of original raw


def test_observer_failure_is_raw_backed_unknown_and_closes(tmp_path):
    provider = NativeProvider(fail='DCPSTopic')
    observer = observe(tmp_path, provider)
    receipt = observer.finish()
    assert provider.closed
    raw = json.loads(Path(receipt['raw']['path']).read_bytes())
    assert raw['status'] == 'unknown'
    assert 'reader_failed' in raw['reasons'][0]
    assert raw['responsibility_layer'] == 'harness'


def test_invalid_admission_cannot_invoke_provider(tmp_path):
    from r2f.runtime.discovery_builtin_observer import DiscoveryBuiltinObserver
    def forbidden():
        raise AssertionError('unauthenticated provider construction')
    observer = DiscoveryBuiltinObserver(tmp_path/'never.json', provider_factory=forbidden)
    with pytest.raises(ValueError, match='admission_required'):
        observer.start(domain_id=19, admission={}, profile={}, environment={})
    assert not (tmp_path/'never.json').exists()


def test_bad_loan_and_cleanup_remain_unknown_with_original_receipts(tmp_path):
    class FailedLoan(NativeProvider):
        def take(self, reader, sample):
            value = super().take(reader, sample)
            value['loan_returned'] = False
            value['loan_cleanup_complete'] = False
            return value
    provider = FailedLoan()
    ref = observe(tmp_path, provider).finish()
    raw = json.loads(Path(ref['raw']['path']).read_bytes())
    assert provider.closed
    assert raw['status'] == 'unknown'
    assert any('loan_incomplete' in reason for reason in raw['reasons'])
    assert raw['receipts'][-2]['receipt']['loan_cleanup_complete'] is False
    assert raw['receipts'][-1]['operation'] == 'close'
