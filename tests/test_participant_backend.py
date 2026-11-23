"""Offline contracts for participant lease facts + the read-only capability probe.

    PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src python3 -m unittest discover \
        -s tests -p test_participant_backend.py -v

No ROS context, DDS participant, domain lock or campaign.  The capability
probe subprocess is doubled with explicit inventories; no ROS/vendor loading.
"""
from contextlib import ExitStack
import json
import subprocess
import unittest
from unittest import mock

from r2f.runtime import participant_backend as pb

GUID_A = 'a1b2c3d4e5f60718293a4b5c6d7e8f90'
GUID_B = '0f1e2d3c4b5a69788796a5b4c3d2e1f0'
IDENT = {'guid': GUID_A, 'vendor': 'cyclonedds', 'generation': 0, 'domain_id': 181}


def lease(**overrides):
    value = {'lease_duration': 30_000_000_000, 'lease_source': 'vendor_config',
             'identity': dict(IDENT)}
    value.update(overrides)
    return value


class ConfigTests(unittest.TestCase):
    def test_lease_domain_accepts_integer_ns_and_infinite(self):
        value = pb.validate_participant_config(lease())
        self.assertTrue(value['accepted'])
        self.assertTrue(value['configured'])
        self.assertEqual(value['lease_duration_ns'], 30_000_000_000)
        self.assertFalse(value['infinite'])
        infinite = pb.validate_participant_config(lease(lease_duration='infinite'))
        self.assertTrue(infinite['infinite'])
        self.assertFalse(infinite['configured'])

    def test_lease_type_and_value_rejections_are_guards(self):
        for duration, layer in ((True, 'config_type'), ('30', 'config_type'),
                                (0, 'config_value'), (-5, 'config_value'),
                                (1.5, 'config_type')):
            with self.subTest(duration=duration):
                value = pb.validate_participant_config(lease(lease_duration=duration))
                self.assertFalse(value['accepted'])
                self.assertEqual(value['layer'], layer)
                self.assertFalse(value['sut_bug'])
                self.assertFalse(value['candidate_eligible'])

    def test_unknown_fields_and_sources_are_rejected(self):
        self.assertEqual(pb.validate_participant_config(lease(surprise=1))['reason'],
                         'unknown_config_field:surprise')
        self.assertEqual(pb.validate_participant_config(lease(lease_source='guess'))['layer'],
                         'evidence')
        missing = pb.validate_participant_config({'identity': dict(IDENT)})
        self.assertEqual(missing['reason'], 'lease_duration_required')

    def test_default_assumption_is_unknown_not_configuration_evidence(self):
        value = pb.validate_participant_config(lease(lease_source='default_assumption'))
        self.assertFalse(value['configured'])
        self.assertEqual(value['capability_status'], 'unknown_not_configured_evidence')

    def test_identity_shape_is_enforced(self):
        for identity, reason in (({'guid': 'zz', 'vendor': 'v'}, 'guid_must_be_16_bytes_hex'),
                                 ({'guid': GUID_A}, 'identity_missing:vendor'),
                                 ('x', 'identity_must_be_object'),
                                 ({'guid': GUID_A, 'vendor': 'v', 'generation': -1},
                                  'generation_must_be_nonnegative_int')):
            with self.subTest(identity=identity):
                value = pb.validate_participant_config(lease(identity=identity))
                self.assertFalse(value['accepted'])
                self.assertEqual(value['reason'], reason)
                self.assertEqual(value['layer'], 'identity')

    def test_liveliness_and_domain_domains(self):
        self.assertEqual(pb.validate_participant_config(lease(liveliness='bad'))['reason'],
                         'unsupported_liveliness:bad')
        self.assertTrue(pb.validate_participant_config(
            lease(liveliness='manual_by_participant'))['accepted'])
        self.assertEqual(pb.validate_participant_config(lease(domain_id=999))['reason'],
                         'domain_id_out_of_range')


class LedgerTests(unittest.TestCase):
    def setUp(self):
        self.records = []
        self.clock_value = 1000
        self.ledger = pb.ParticipantLedger(emit=self.records.append,
                                           clock=lambda: self.clock_value)

    def register(self, identity=None, **kwargs):
        return self.ledger.register(identity or dict(IDENT), lease(), **kwargs)

    def test_register_records_config_without_creating_participant(self):
        value = self.register()
        self.assertEqual(value['receipt_kind'], 'participant_registered')
        self.assertTrue(value['configured'])
        self.assertFalse(value['participant_created'])
        self.assertEqual(value['registration_basis'], 'declared_identity')
        self.assertFalse(value['candidate_eligible'])

    def test_config_guard_blocks_registration(self):
        value = self.ledger.register(dict(IDENT), {'lease_duration': 0})
        self.assertEqual(value['receipt_kind'], 'participant_guard')
        self.assertIsNone(self.ledger.current)

    def test_activity_refresh_is_basis_named(self):
        self.register()
        value = self.ledger.activity(dict(IDENT), at_ns=2000)
        self.assertFalse(value['refresh_observed'])
        self.assertEqual(value['refresh_basis'], 'builtin_participant_sample')
        self.assertFalse(value['participant_lease_evidence'] if 'participant_lease_evidence' in value else False)

    def test_staleness_never_infers_expiry(self):
        self.register()
        value = self.ledger.silence(dict(IDENT), window_ns=5_000_000_000)
        self.assertEqual(value['status'], 'stale_unconfirmed')
        self.assertFalse(value['expiry_observed'])
        self.assertFalse(value['expiry_inferred'])
        self.assertFalse(value['writer_liveliness_used'])
        state = self.ledger.state()
        self.assertEqual(state['status'], 'registered_no_activity')

    def test_disposal_and_no_writers_are_disappearance_not_expiry(self):
        for basis, instance_state in (("builtin_instance_disposed", "NOT_ALIVE_DISPOSED"),
                                      ("builtin_instance_no_writers", "NOT_ALIVE_NO_WRITERS")):
            ledger = pb.ParticipantLedger(clock=lambda: 3000)
            ledger.register(dict(IDENT), lease())
            evidence = [{'guid': GUID_A, 'generation': IDENT['generation'], 'vendor': IDENT['vendor'], 'instance_state': instance_state, 'valid_data': False}]
            value = ledger.expiry(dict(IDENT), basis=basis, evidence=evidence)
            self.assertEqual(value['receipt_kind'], 'participant_disappearance')
            self.assertFalse(value['disappearance_observed'])
            self.assertFalse(value['expiry_observed'])
            self.assertEqual(value['cause'], 'unknown')
            state = ledger.state()
            self.assertEqual(state['status'], 'disappearance_declared')
            self.assertIsNone(state['expiry'])
            self.assertFalse(state['expiry_observed'])
            self.assertEqual(state['expiry_cause_status'], 'unknown_no_authoritative_cause_evidence')

    def test_disappearance_requires_matching_identity_state_and_invalid_data(self):
        self.register()
        for evidence in (None, [{'guid': GUID_B, 'instance_state': 'NOT_ALIVE_DISPOSED', 'valid_data': False}],
                         [{'guid': GUID_A, 'instance_state': 'NOT_ALIVE_NO_WRITERS', 'valid_data': False}],
                         [{'guid': GUID_A, 'instance_state': 'NOT_ALIVE_DISPOSED', 'valid_data': True}]):
            value = self.ledger.expiry(dict(IDENT), evidence=evidence)
            self.assertEqual(value['reason'], 'disappearance_requires_matching_builtin_evidence')
            self.assertFalse(value.get('expiry_observed', False))
        self.assertEqual(self.ledger.state()['status'], 'registered_no_activity')

    def test_unverified_expiry_cause_never_promotes_caller_assertion(self):
        self.register()
        evidence = [{'guid': GUID_A, 'cause': 'lease_expired', 'raw_ref': 'claimed'}]
        value = self.ledger.expiry(dict(IDENT), basis='vendor_lease_expired', evidence=evidence)
        self.assertEqual(value['reason'], 'expiry_cause_verifier_unavailable')
        self.assertFalse(value['expiry_observed'])
        self.assertEqual(self.ledger.state()['status'], 'registered_no_activity')

    def test_disappearance_then_rediscovery_does_not_imply_expiry(self):
        self.register()
        self.ledger.disappearance(dict(IDENT), evidence=[{'guid': GUID_A,
            'generation': IDENT['generation'], 'vendor': IDENT['vendor'],
            'instance_state': 'NOT_ALIVE_DISPOSED', 'valid_data': False}], at_ns=1000)
        self.assertFalse(self.ledger.rediscovery(dict(IDENT), at_ns=1001)['prior_gap_observed'])
        state = self.ledger.state()
        self.assertFalse(state['expiry_observed'])
        self.assertIsNone(state['expiry'])

    def test_rediscovery_after_proven_gap(self):
        self.register()
        self.ledger.activity(dict(IDENT), at_ns=2000)
        self.ledger.silence(dict(IDENT), window_ns=1_000_000)
        value = self.ledger.rediscovery(dict(IDENT), at_ns=4000)
        self.assertFalse(value['prior_gap_observed'])
        self.assertFalse(value['rediscovery_observed'])
        self.assertTrue(value['identity_generation_unchanged'])
        fresh = pb.ParticipantLedger()
        fresh.register(dict(IDENT), lease())
        no_gap = fresh.rediscovery(dict(IDENT), at_ns=10)
        self.assertFalse(no_gap['prior_gap_evidence'])
        self.assertEqual(no_gap['claim'], 'reappearance_without_recorded_gap')

    def test_identity_replacement_is_not_refresh(self):
        self.register()
        value = self.ledger.replace_identity(
            {'guid': GUID_B, 'vendor': 'cyclonedds', 'generation': 1})
        self.assertFalse(value['replacement_observed'])
        self.assertFalse(value['counts_as_refresh'])
        self.assertFalse(value['old_lease_reused'])
        self.assertEqual(value['new_generation'], 1)
        self.assertEqual(self.ledger.current['guid'], GUID_B)
        same = self.ledger.replace_identity(
            {'guid': GUID_B, 'vendor': 'cyclonedds', 'generation': 2})
        self.assertEqual(same['reason'], 'replacement_requires_different_guid')

    def test_writer_liveliness_is_a_separate_channel(self):
        self.register()
        value = self.ledger.writer_liveliness(dict(IDENT), policy='manual_by_topic',
                                              at_ns=5000)
        self.assertFalse(value['participant_lease_evidence'])
        self.assertEqual(value['channel'], 'writer_liveliness')
        self.assertFalse(value['refresh_claimed'])
        self.assertEqual(self.ledger.state()['status'], 'registered_no_activity')
        bad = self.ledger.writer_liveliness(dict(IDENT), policy='nope')
        self.assertEqual(bad['layer'], 'config_value')

    def test_mismatch_cannot_be_inferred_as_expiry(self):
        self.register()
        value = self.ledger.infer_expiry_from_mismatch(mismatch_ns=9999)
        self.assertEqual(value['reason'], 'mismatch_or_silence_cannot_prove_lease_expiry')
        self.assertEqual(value['layer'], 'channel_confusion')
        self.assertFalse(value['expiry_inferred'])

    def test_wrong_guid_and_unregistered_are_precondition_guards(self):
        self.assertEqual(self.ledger.activity(dict(IDENT))['reason'],
                         'participant_not_registered')
        self.register()
        value = self.ledger.activity({'guid': GUID_B, 'vendor': 'cyclonedds', 'generation': 0})
        self.assertEqual(value['reason'], 'identity_not_current')
        self.assertEqual(value['layer'], 'identity')

    def test_activity_requires_known_source_and_valid_time(self):
        self.register()
        self.assertEqual(self.ledger.activity(dict(IDENT), source='guess')['layer'], 'evidence')
        self.assertEqual(self.ledger.activity(dict(IDENT), at_ns=-1)['reason'],
                         'activity_time_invalid')

    def test_state_basis_is_always_named(self):
        self.assertEqual(self.ledger.state()['state_basis'], 'no_registration')
        self.register()
        self.assertEqual(self.ledger.state()['state_basis'], 'registration_only')
        self.ledger.activity(dict(IDENT), at_ns=2000)
        state = self.ledger.state()
        self.assertEqual(state['status'], 'activity_declared')
        self.assertEqual(state['state_basis'], 'caller_declared_activity')
        self.assertFalse(state['expiry_inferred'])

    def test_ordered_declarations_and_no_old_recovery(self):
        self.register()
        e = [{'guid': GUID_A, 'vendor': IDENT['vendor'], 'generation': IDENT['generation'],
              'instance_state': 'NOT_ALIVE_DISPOSED', 'valid_data': False, 'raw_ref': 'unverified'}]
        first = self.ledger.disappearance(dict(IDENT), at_ns=10, evidence=e)
        self.assertEqual(first['stage'], 'declared')
        self.assertFalse(first['disappearance_observed'])
        self.ledger.rediscovery(dict(IDENT), at_ns=11, evidence=[{'raw_ref': 'unverified'}])
        self.assertEqual(self.ledger.state()['status'], 'rediscovery_declared')
        self.ledger.disappearance(dict(IDENT), at_ns=12, evidence=e)
        state = self.ledger.state()
        self.assertEqual(state['status'], 'disappearance_declared')
        self.assertIsNone(state['rediscovery'])
        self.assertEqual(state['disappearance']['disappearance_ns'], 12)
        self.assertEqual(self.ledger.rediscovery(dict(IDENT), at_ns=11)['reason'], 'event_time_not_after_previous')
        self.assertEqual(self.ledger.rediscovery(dict(IDENT), at_ns=-1)['reason'], 'rediscovery_time_invalid')
        self.assertEqual(self.ledger.state()['status'], 'disappearance_declared')

    def test_identity_and_evidence_fail_closed(self):
        self.register()
        for identity in ({'guid': GUID_A, 'vendor': IDENT['vendor']},
                         dict(IDENT, generation=IDENT['generation'] + 1),
                         dict(IDENT, vendor='other')):
            self.assertEqual(self.ledger.activity(identity, at_ns=10)['receipt_kind'], 'participant_guard')
        for evidence in (None, [{'guid': GUID_A, 'instance_state': 'NOT_ALIVE_DISPOSED', 'valid_data': False}]):
            self.assertEqual(self.ledger.disappearance(dict(IDENT), at_ns=10, evidence=evidence)['receipt_kind'], 'participant_guard')
        self.assertEqual(self.ledger.state()['status'], 'registered_no_activity')

    def test_metadata_changes_cannot_reset_chronology_or_projection(self):
        original = dict(IDENT, domain_id=1, participant_name='old')
        changed = dict(IDENT, domain_id=2, participant_name='new')
        self.register(original)
        evidence = [{'guid': GUID_A, 'vendor': IDENT['vendor'],
                     'generation': IDENT['generation'],
                     'instance_state': 'NOT_ALIVE_DISPOSED', 'valid_data': False}]
        self.ledger.disappearance(original, at_ns=100, evidence=evidence)
        self.assertEqual(self.ledger.rediscovery(changed, at_ns=99)['reason'],
                         'event_time_not_after_previous')
        self.assertEqual(self.ledger.state()['status'], 'disappearance_declared')
        self.assertTrue(self.ledger.register(changed, lease())['receipt_kind'] == 'participant_registered')
        self.assertEqual(self.ledger.state()['status'], 'disappearance_declared')
        self.assertEqual(self.ledger.activity(original, at_ns=100)['reason'],
                         'event_time_not_after_previous')
        recovery = self.ledger.rediscovery(changed, at_ns=101)
        self.assertTrue(recovery['prior_gap_declared'])
        self.assertFalse(recovery['rediscovery_observed'])
        self.assertEqual(self.ledger.state()['status'], 'rediscovery_declared')
        self.ledger.disappearance(original, at_ns=102, evidence=evidence)
        self.assertEqual(self.ledger.state()['status'], 'disappearance_declared')
        self.assertIsNone(self.ledger.state()['rediscovery'])
        self.assertEqual(self.ledger.rediscovery(changed, at_ns=101)['reason'],
                         'event_time_not_after_previous')

    def test_ledger_never_counts_toward_maturity(self):
        self.register()
        self.ledger.activity(dict(IDENT))
        for item in self.records:
            self.assertFalse(item['counts_toward_maturity'])
            self.assertFalse(item['candidate_eligible'])
            self.assertFalse(item['mutation_receipt'])


class CapabilityProbeTests(unittest.TestCase):
    def setUp(self):
        stack = ExitStack()
        self.addCleanup(stack.close)
        # Explicit child-process output, not a claim about the CI host stack.
        self.inventory = {
            'public_api': {
                'rclpy_import': True,
                'create_subscription_has_content_filter_options': True,
                'subscription_set_content_filter': True,
                'subscription_get_content_filter': True,
                'subscription_is_cft_enabled': True,
                'publisher_assert_liveliness': True,
                'content_filter_options_fields': [
                    'filter_expression', 'expression_parameters'],
                'context_created': False, 'node_created': False,
                'participant_created': False},
            'packages': {
                'rclpy': {'version': 'fixture-version'},
                'rmw_cyclonedds_cpp': {'package_xml_sha256': 'a' * 64}},
            'ddsc': {
                'available': True, 'participant_created': False,
                'library': '/fixture/libddsc.so.0',
                'realpath': '/fixture/libddsc.so.fixture',
                'sha256': 'b' * 64,
                'symbols': {name: True for name in (
                    'dds_create_participant', 'dds_delete', 'dds_get_guid',
                    'dds_create_reader', 'dds_create_topic', 'dds_take',
                    'dds_return_loan', 'dds_builtintopic_free_participant',
                    'dds_qget_liveliness', 'dds_qget_history')}},
            'errors': []}
        self.process = subprocess.CompletedProcess(
            args=[], returncode=0, stdout=json.dumps(self.inventory), stderr='')
        self.run = stack.enter_context(mock.patch.object(
            pb.subprocess, 'run', return_value=self.process))
        self.value = pb.capability_probe()
        self.run.assert_called_once()
        self.assertEqual(self.run.call_args.args,
                         ([pb.sys.executable, '-B', '-c', pb._probe_code()],))
        self.assertTrue(self.run.call_args.kwargs['capture_output'])
        self.assertTrue(self.run.call_args.kwargs['text'])

    def test_probe_runs_without_creating_entities(self):
        self.assertTrue(self.value['ok'], self.value)
        self.assertEqual(self.value['capability_status'], 'observed')
        self.assertFalse(self.value['participant_created'])
        self.assertFalse(self.value['context_created'])
        capabilities = self.value['capabilities']
        self.assertFalse(capabilities['public_api']['context_created'])
        self.assertFalse(capabilities['public_api']['node_created'])
        self.assertFalse(capabilities['public_api']['participant_created'])
        self.assertFalse(capabilities['ddsc']['participant_created'])

    def test_public_filter_api_and_liveliness_are_available(self):
        api = self.value['capabilities']['public_api']
        self.assertTrue(api['create_subscription_has_content_filter_options'])
        self.assertTrue(api['subscription_set_content_filter'])
        self.assertTrue(api['subscription_get_content_filter'])
        self.assertTrue(api['subscription_is_cft_enabled'])
        self.assertTrue(api['publisher_assert_liveliness'])
        self.assertEqual(self.value['public_api_gaps'], [])
        self.assertEqual(api['content_filter_options_fields'],
                         ['filter_expression', 'expression_parameters'])

    def test_versions_and_library_hashes_are_recorded(self):
        packages = self.value['capabilities']['packages']
        self.assertEqual(packages, self.inventory['packages'])
        self.assertEqual(len(packages['rmw_cyclonedds_cpp']['package_xml_sha256']), 64)
        ddsc = self.value['capabilities']['ddsc']
        self.assertTrue(ddsc['available'])
        self.assertEqual(ddsc['sha256'], self.inventory['ddsc']['sha256'])
        self.assertTrue(ddsc['library'].endswith('libddsc.so.0'))
        self.assertTrue(all(ddsc['symbols'].values()))

    def test_native_route_is_declared_without_compiler(self):
        route = self.value['native_route']
        self.assertEqual(route['backend'], 'ctypes-cyclonedds')
        self.assertFalse(route['compiler_required'])
        self.assertEqual(route['live_create_destroy'], 'not_run_no_live_go')
        self.assertEqual(route['library_sha256'], self.inventory['ddsc']['sha256'])
        self.assertEqual(route['library'], self.inventory['ddsc']['realpath'])

    def test_probe_receipt_is_not_execution_evidence(self):
        self.assertFalse(self.value['counts_toward_maturity'])
        self.assertFalse(self.value['candidate_eligible'])
        self.assertFalse(self.value['mutation_receipt'])
        self.assertEqual(len(self.value['source_identity']['sha256']), 64)

    def test_failed_probe_is_an_unknown_capability_guard(self):
        self.run.reset_mock()
        self.run.side_effect = subprocess.TimeoutExpired('fixture-probe', 0.01)
        value = pb.capability_probe(timeout_sec=0.01)
        self.run.assert_called_once()
        self.assertEqual(self.run.call_args.kwargs['timeout'], 0.01)
        self.assertFalse(value['ok'])
        self.assertEqual(value['capability_status'], 'probe_failed')
        self.assertEqual(value['layer'], 'unknown_capability')
        self.assertFalse(value['participant_created'])
        self.assertIsNone(value['native_ddsc_available'])
        self.assertIsNone(value['native_route']['available'])
        self.assertEqual(value['native_route']['live_create_destroy'], 'unknown')

    def test_unavailable_features_are_explicit_gaps_not_probe_failure(self):
        absent = {
            'public_api': {'rclpy_import': False,
                           'context_created': False, 'node_created': False,
                           'participant_created': False},
            'ddsc': {'available': False, 'error': 'fixture:library_unavailable',
                     'participant_created': False},
            'errors': ['rclpy:ModuleNotFoundError', 'ddsc:OSError']}
        self.process.stdout = json.dumps(absent)
        value = pb.capability_probe()
        self.assertTrue(value['ok'])  # Inventory succeeded; features are absent.
        self.assertEqual(value['capability_status'], 'observed')
        self.assertEqual(value['capabilities'], absent)
        self.assertFalse(value['public_filter_api'])
        self.assertFalse(value['native_ddsc_available'])
        self.assertEqual(value['public_api_gaps'], [
            'subscription_set_content_filter', 'subscription_get_content_filter',
            'subscription_is_cft_enabled', 'publisher_assert_liveliness'])
        self.assertEqual(value['native_route'], {
            'backend': 'ctypes-cyclonedds', 'available': False,
            'reason': 'fixture:library_unavailable',
            'live_create_destroy': 'capability_gap'})
        self.assertFalse(value['participant_created'])
        self.assertFalse(value['context_created'])
        self.assertFalse(value['candidate_eligible'])
        self.assertFalse(value['counts_toward_maturity'])
        self.assertFalse(value['mutation_receipt'])


if __name__ == '__main__':
    unittest.main()
