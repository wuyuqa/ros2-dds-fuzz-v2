"""Finite builtin-reader helper within the projection namespace lifecycle.

The backend alone owns native entities and reader loans. Importing this module
never loads a library. Production construction belongs after namespace admission.
"""
from copy import deepcopy
import hashlib
import json
import os
import re
from pathlib import Path
import time


class DiscoveryBuiltinObserver:
    def __init__(self, path, *, provider_factory=None):
        self.path = Path(path)
        self.factory = provider_factory
        self.backend = None
        self.readers = {}
        self.reference = None
        self.raw = {'schema': 'r2f.discovery-builtins/v1', 'status': 'unknown',
                    'responsibility_layer': 'harness', 'reasons': [], 'receipts': [],
                    'samples': {'DCPSParticipant': [], 'DCPSTopic': []},
                    'cleanup_complete': False, 'candidate_eligible': False}

    def _record(self, operation, value):
        row = {'operation': operation, 'unix_ns': time.time_ns(),
               'monotonic_ns': time.monotonic_ns(), 'receipt': deepcopy(value)}
        self.raw['receipts'].append(row)
        if value.get('status') == 'blocked' or value.get('accepted') is False:
            raise RuntimeError(value.get('reason', 'native_api_failed'))
        return value

    def start(self, *, domain_id, admission, profile, environment):
        # No provider, import or native open occurs on an unauthenticated call.
        if (admission.get('event') != 'namespace_host_admission_verified'
                or not admission.get('raw_event_ref')
                or type(domain_id) is not int or admission.get('domain_id') != domain_id):
            raise ValueError('projection_native_admission_required')
        if self.backend is not None or 'started_unix_ns' in self.raw:
            raise ValueError('projection_native_already_started')
        self.raw.update(started_unix_ns=time.time_ns(), started_monotonic_ns=time.monotonic_ns(),
                        timestamp_clock='unix_ns_realtime', domain_id=domain_id, admission=deepcopy(admission),
                        profile=deepcopy(profile), environment=deepcopy(environment),
                        pid=os.getpid())
        previous = os.environ.get('CYCLONEDDS_URI')
        try:
            data = Path(profile['path']).read_bytes()
            if hashlib.sha256(data).hexdigest() != profile['sha256']:
                raise ValueError('native_profile_hash_mismatch')
            if (environment != {'CYCLONEDDS_URI': profile['path']}
                    or b'<Discovery><EnableTopicDiscoveryEndpoints>true</EnableTopicDiscoveryEndpoints>' not in data):
                raise ValueError('native_topic_discovery_profile_required')
            os.environ.update(environment)
            if self.factory is None:
                from .native_participant import NativeParticipantBackend
                self.backend = NativeParticipantBackend()
            else:
                self.backend = self.factory()
            opened = self._record('open', self.backend.open({
                'enabled': True, 'domain_id': domain_id,
                'live_authorization': {'granted': True, 'source': admission['raw_event_ref']}}))
            if (opened.get('receipt_kind') != 'native_participant_created'
                    or opened.get('real_execution') is not True
                    or type(opened.get('participant_handle')) is not int or opened['participant_handle'] <= 0
                    or opened.get('domain_id') != domain_id
                    or not isinstance(opened.get('guid'), str)
                    or re.fullmatch('[0-9a-f]{32}', opened['guid']) is None):
                raise RuntimeError('native_open_identity_or_domain_unknown')
            self.raw['observer_guid'] = opened.get('guid')
            for topic in self.raw['samples']:
                reader = self._record('reader:' + topic, self.backend.builtin_reader(topic=topic))
                if (reader.get('receipt_kind') != 'native_builtin_reader_created'
                        or reader.get('real_execution') is not True or reader.get('topic') != topic
                        or reader.get('topic_handle') != (0x7fff0001 if topic == 'DCPSParticipant' else 0x7fff0002)
                        or type(reader.get('reader_handle')) is not int or reader['reader_handle'] <= 0
                        or reader['reader_handle'] in self.readers.values()):
                    raise RuntimeError('native_reader_identity_unknown:' + topic)
                self.readers[topic] = reader['reader_handle']
        except Exception as exc:
            self.raw['reasons'].append(type(exc).__name__ + ':' + str(exc))
            self._close()
        finally:
            if previous is None:
                os.environ.pop('CYCLONEDDS_URI', None)
            else:
                os.environ['CYCLONEDDS_URI'] = previous

    def _close(self):
        if self.backend is not None:
            backend, self.backend = self.backend, None
            try:
                closed = self._record('close', backend.close())
                self.raw['cleanup_complete'] = closed.get('cleanup_complete') is True
                if not self.raw['cleanup_complete']:
                    self.raw['reasons'].append('native_cleanup_incomplete')
            except Exception as exc:
                self.raw['reasons'].append('native_cleanup_failed:' + str(exc))
        self.raw['closed_unix_ns'] = time.time_ns()
        self.raw['closed_monotonic_ns'] = time.monotonic_ns()

    def finish(self):
        if self.reference is not None:
            return deepcopy(self.reference)
        try:
            if self.backend is not None:
                for topic, reader in self.readers.items():
                    take = (self.backend.take_participants if topic == 'DCPSParticipant'
                            else self.backend.take_topics)
                    for _ in range(64):
                        value = self._record('take:' + topic, take(reader))
                        if (value.get('reader_handle') != reader
                                or value.get('receipt_kind') != ('native_builtin_participants'
                                    if topic == 'DCPSParticipant' else 'native_builtin_topics')
                                or value.get('accepted') is not True or value.get('ok') is not True
                                or value.get('loan_cleanup_complete') is not True
                                or (value.get('take_return_code', 0) > 0
                                    and value.get('loan_returned') is not True)):
                            raise RuntimeError('native_take_or_loan_incomplete:' + topic)
                        self.raw['samples'][topic].extend(deepcopy(value['samples']))
                        if value['take_return_code'] == 0:
                            break
                    else:
                        raise RuntimeError('native_take_drain_limit:' + topic)
        except Exception as exc:
            self.raw['reasons'].append(type(exc).__name__ + ':' + str(exc))
        finally:
            self._close()
        if not self.raw['reasons'] and self.raw['cleanup_complete']:
            if all(self.raw['samples'].values()):
                self.raw['status'] = 'observed'
            else:
                self.raw['reasons'].append('builtin_samples_unavailable')
        data = json.dumps(self.raw, sort_keys=True).encode()
        with self.path.open('xb') as handle:
            handle.write(data)
        self.reference = {'schema': self.raw['schema'], 'status': self.raw['status'],
                          'raw': {'path': str(self.path), 'sha256': hashlib.sha256(data).hexdigest()}}
        return deepcopy(self.reference)
