"""CP1 offline qualification only: authored native API values, no DDS load/run."""
import ctypes as c
from pathlib import Path

import pytest

from r2f.runtime import topology_worker as worker
from r2f.runtime.topology_worker import CycloneMatchedPeerObserver
from test_topology_peer_unavailable import snapshot_worker

ENDPOINT = '112233445566778899aabbccddeeff00'
# Intentionally no shared prefix: ownership must come from the API.
PARTICIPANT = 'fedcba98765432100123456789abcdef'


class NativeDouble:
    """Read-only Cyclone API boundary with implicit publisher between entities."""
    def __init__(self):
        self.roots = [10]
        self.children = {10: [20], 20: [30], 30: []}
        self.guids = {10: PARTICIPANT, 20: 'a' * 32, 30: ENDPOINT}
        self.parents = {10: 0, 20: 10, 30: 20}
        self.domains = {10: 196, 20: 196, 30: 196}

    def dds_lookup_participant(self, domain, array, capacity):
        return self.copy(self.roots, array, capacity)

    def dds_get_children(self, handle, array, capacity):
        return self.copy(self.children[handle], array, capacity)

    @staticmethod
    def copy(values, array, capacity):
        for i, value in enumerate(values[:capacity]):
            array[i] = value
        return len(values)

    def dds_get_guid(self, handle, ptr):
        if handle == 20:
            return -12  # implicit publisher has no network GUID (dds.h).
        ptr._obj.v[:] = bytes.fromhex(self.guids[handle])
        return 0

    def dds_get_domainid(self, handle, ptr):
        ptr._obj.value = self.domains[handle]
        return 0

    def dds_get_parent(self, handle):
        return self.parents[handle]

    def dds_get_participant(self, handle):
        return 10

    def dds_get_matched_subscriptions(self, handle, array, capacity):
        return 0

    dds_get_matched_publications = dds_get_matched_subscriptions


@pytest.fixture
def native(monkeypatch):
    # Constructor mapping/ABI qualification is existing code; never dlopen here.
    monkeypatch.setattr(c, 'CDLL', lambda *a, **kw: pytest.fail('unexpected dlopen'))
    class Guid(c.Structure):
        _fields_ = [('v', c.c_uint8 * 16)]
    observer = CycloneMatchedPeerObserver.__new__(CycloneMatchedPeerObserver)
    observer.c, observer.Guid = c, Guid
    observer.lib = NativeDouble()
    for name in ('dds_get_parent', 'dds_get_participant'):
        method = getattr(observer.lib, name)
        setattr(observer.lib, name, lambda handle, method=method: method(handle))
    observer.provenance = {'fixture': True, 'creates_entities': False}
    return observer


def test_actual_implicit_parent_chain_identifies_participant(native):
    receipt = native.sample_ownership(196, ENDPOINT, 'writer')
    assert receipt['complete'] is True
    assert receipt['status'] == 'observed'
    assert receipt['participant_gid'] == PARTICIPANT
    assert receipt['local_gid'] == ENDPOINT
    assert receipt['domain_id'] == 196
    assert receipt['parent_chain'] == [30, 20, 10]
    assert receipt['candidate_eligible'] is False
    assert receipt['ownership_api'] == 'dds_get_participant+dds_get_parent'
    assert receipt['provenance']['creates_entities'] is False


def test_participant_domain_must_be_observed_not_assumed_from_endpoint(native):
    native.lib.domains[10] = 197
    receipt = native.sample_ownership(196, ENDPOINT, 'writer')
    assert receipt['complete'] is False
    assert receipt['reason'] == 'participant_inventory_domain_mismatch'
    assert 'participant_gid' not in receipt


@pytest.mark.parametrize('guid', [None, '0' * 32, ENDPOINT])
def test_inventory_requires_unique_nonzero_guid_readback(native, guid):
    original = native.lib.dds_get_guid
    def read(handle, ptr):
        if handle == 10:
            if guid is None:
                return -1
            ptr._obj.v[:] = bytes.fromhex(guid)
            return 0
        return original(handle, ptr)
    native.lib.dds_get_guid = read
    receipt = native.sample_ownership(196, ENDPOINT, 'reader')
    assert receipt['complete'] is False
    expected = ('local_gid_not_unique_in_existing_entities' if guid == ENDPOINT
                else 'participant_inventory_guid_invalid_or_aliased')
    assert receipt['reason'] == expected
    assert 'participant_gid' not in receipt


# Reuse the existing public worker harness (ROS/pipe boundary doubles).
@pytest.fixture
def worker_guards(monkeypatch):
    original = Path.read_text
    def read(path, *args, **kwargs):
        if str(path) == '/proc/self/stat':
            return '101 (offline) ' + ' '.join(['0'] * 19 + ['9876'])
        if str(path).startswith('/proc/'):
            pytest.fail('unexpected proc inspection: ' + str(path))
        return original(path, *args, **kwargs)
    monkeypatch.setattr(Path, 'read_text', read)
    monkeypatch.setattr(worker, '_monotonic_clock_identity', lambda: {'fixture': True})
    monkeypatch.setattr(worker.subprocess, 'Popen', lambda *a, **k: pytest.fail('unexpected process'))
    import socket
    monkeypatch.setattr(socket, 'socket', lambda *a, **k: pytest.fail('unexpected network'))
    monkeypatch.setattr(worker.os, 'getpgrp', lambda: 555)


@pytest.mark.parametrize('kind', ['writer', 'reader'])
@pytest.mark.parametrize('root_form', ['nil', 'domain'])
def test_worker_emits_ownership_with_current_envelope_without_changing_counts(
        native, monkeypatch, worker_guards, snapshot_worker, kind, root_form):
    if root_form == 'domain':
        native.lib.parents.update({10: 90, 90: 99, 99: 0})
        native.lib.children[90] = [10]
        native.lib.domains[90] = 196
        native.lib.dds_get_participant = lambda handle: 0 if handle in (90, 99) else 10
    gid = '1' * 32 if kind == 'writer' else '2' * 32
    native.lib.guids[30] = gid
    native.lib.dds_get_matched_subscription_data = lambda *args: None
    native.lib.dds_get_matched_publication_data = lambda *args: None
    monkeypatch.setattr(worker, 'CycloneMatchedPeerObserver', lambda: native)
    rows = snapshot_worker(kind, rmw='rmw_cyclonedds_cpp', count=0)
    ownership = next(row for row in rows if row['event'] == 'participant_ownership_snapshot')
    created = next(row for row in rows if row['event'] == 'endpoint_created')
    for key in ('run_id', 'domain_id', 'observed_domain_id', 'logical_id', 'generation',
                'pid', 'pgid', 'process_start_ticks', 'endpoint_generation',
                'rmw_identifier', 'observation_mode', 'endpoint_gid'):
        assert ownership[key] == created[key]
    assert ownership['command_id'] == 1
    receipt = ownership['participant_ownership_observation']
    assert receipt['participant_gid'] == PARTICIPANT
    assert receipt['local_gid'] == ownership['endpoint_gid'] == gid
    assert receipt['candidate_eligible'] is False
    assert ownership['candidate_eligible'] is False
    assert ownership['supporting_diagnostic'] is True
    snapshot = next(row for row in rows if row['event'] == 'match_snapshot')
    assert snapshot['matched_count'] == snapshot['public_count_before'] == snapshot['public_count_after'] == 0
    assert any(row['event'] == 'command_done' and row['command_id'] == 1 for row in rows)


def test_guid_race_inside_each_inventory_cannot_bind_stale_endpoint(native):
    reads = 0
    original = native.lib.dds_get_guid
    def raced(handle, ptr):
        nonlocal reads
        if handle == 30:
            reads += 1
            if reads % 2 == 0:
                ptr._obj.v[:] = bytes.fromhex('b' * 32)
                return 0
        return original(handle, ptr)
    native.lib.dds_get_guid = raced
    receipt = native.sample_ownership(196, ENDPOINT, 'writer')
    assert receipt['complete'] is False
    assert receipt['reason'] == 'participant_endpoint_guid_changed'
    assert 'participant_gid' not in receipt


@pytest.mark.parametrize('damage,reason', [
    ('missing_participant_api', 'participant_ownership_api_unavailable:dds_get_participant'),
    ('missing_parent_api', 'participant_ownership_api_unavailable:dds_get_parent'),
    ('endpoint_domain', 'local_entity_domain_mismatch'),
    ('endpoint_missing', 'local_gid_not_unique_in_existing_entities'),
    ('duplicate_guid', 'local_gid_not_unique_in_existing_entities'),
    ('duplicate_roots', 'native_handles_invalid_or_aliased'),
    ('cycle', 'entity_inventory_cycle_alias_or_capacity_exceeded'),
    ('capacity', 'native_enumeration_error_or_capacity_exceeded'),
    ('tree_capacity', 'entity_inventory_cycle_alias_or_capacity_exceeded'),
    ('parent_cycle', 'participant_parent_chain_invalid'),
    ('wrong_participant', 'participant_root_invalid'),
    ('participant_error', 'participant_lookup_failed'),
    ('root_parent', 'participant_root_invalid'),
    ('native_error', 'fixture_native_read_error'),
])
def test_unavailable_ambiguous_and_invalid_ownership_never_produces_proof(native, damage, reason):
    lib = native.lib
    if damage == 'missing_participant_api': lib.dds_get_participant = None
    elif damage == 'missing_parent_api': lib.dds_get_parent = None
    elif damage == 'endpoint_domain': lib.domains[30] = 197
    elif damage == 'endpoint_missing': lib.guids[30] = 'b' * 32
    elif damage == 'duplicate_guid': lib.guids[10] = ENDPOINT
    elif damage == 'duplicate_roots': lib.roots = [10, 10]
    elif damage == 'cycle': lib.children[30] = [10]
    elif damage == 'capacity': lib.dds_lookup_participant = lambda *args: 513
    elif damage == 'tree_capacity': native.MAX_ENTITIES = 2
    elif damage == 'parent_cycle': lib.parents[30] = 30
    elif damage == 'wrong_participant': lib.dds_get_participant = lambda handle: 20
    elif damage == 'participant_error': lib.dds_get_participant = lambda handle: -9
    elif damage == 'root_parent': lib.parents[10] = 20
    elif damage == 'native_error':
        def fail(*args): raise OSError('fixture_native_read_error')
        lib.dds_lookup_participant = fail
    receipt = native.sample_ownership(196, ENDPOINT, 'writer')
    assert receipt['complete'] is False
    assert receipt['status'] == ('unavailable' if damage.startswith('missing_') else 'unknown')
    assert receipt['reason'] == reason
    assert receipt['candidate_eligible'] is False
    assert 'participant_gid' not in receipt


def test_two_independent_participants_do_not_make_unique_endpoint_ownership_ambiguous(native):
    native.lib.roots = [10, 40]
    native.lib.children[40] = []
    native.lib.guids[40] = 'c' * 32
    native.lib.domains[40] = 196
    receipt = native.sample_ownership(196, ENDPOINT, 'writer')
    assert receipt['complete'] is True
    assert receipt['participant_gid'] == PARTICIPANT


def test_changed_inventory_between_samples_is_unknown(native):
    calls = 0
    original = native.lib.dds_get_participant
    def raced(handle):
        nonlocal calls
        if handle == 30:  # change between endpoint captures, not root self-checks
            calls += 1
            if calls == 2:
                native.lib.guids[10] = 'd' * 32
        return original(handle)
    native.lib.dds_get_participant = raced
    receipt = native.sample_ownership(196, ENDPOINT, 'writer')
    assert receipt['complete'] is False
    assert receipt['reason'] == 'participant_ownership_inventory_changed'
    assert 'participant_gid' not in receipt


def test_recreated_endpoint_cannot_reuse_old_guid_ownership(native):
    assert native.sample_ownership(196, ENDPOINT, 'writer')['complete'] is True
    native.lib.guids[30] = 'b' * 32  # reused handle, different real endpoint GUID
    stale = native.sample_ownership(196, ENDPOINT, 'writer')
    fresh = native.sample_ownership(196, 'b' * 32, 'writer')
    assert stale['complete'] is False
    assert 'participant_gid' not in stale
    assert fresh['complete'] is True and fresh['local_gid'] == 'b' * 32
    assert 'generation' not in fresh  # actual API does not expose lifecycle generation


@pytest.mark.parametrize('domain,gid,kind', [
    (True, ENDPOINT, 'writer'), (-1, ENDPOINT, 'writer'), (2**32, ENDPOINT, 'writer'),
    (196, '0' * 32, 'reader'), (196, 'configured_participant', 'writer'),
    (196, ENDPOINT, 'participant'),
])
def test_invalid_identity_does_not_query_native_api(native, domain, gid, kind):
    native.lib.dds_lookup_participant = lambda *args: pytest.fail('unexpected native query')
    receipt = native.sample_ownership(domain, gid, kind)
    assert receipt['complete'] is False
    assert receipt['reason'] == 'participant_ownership_input_identity_invalid'


@pytest.mark.parametrize('case', ['missing_api', 'capture_error', 'fastdds_blackbox'])
def test_ownership_failure_preserves_match_and_command_ack(
        native, monkeypatch, worker_guards, snapshot_worker, case):
    native.lib.guids[30] = '1' * 32
    native.lib.dds_get_matched_subscription_data = lambda *args: None
    if case == 'missing_api': native.lib.dds_get_parent = None
    elif case == 'capture_error':
        def fail(*args): raise RuntimeError('fixture_ownership_read_failed')
        native.sample_ownership = fail
    monkeypatch.setattr(worker, 'CycloneMatchedPeerObserver', lambda: native)
    rmw = 'rmw_fastrtps_cpp' if case == 'fastdds_blackbox' else 'rmw_cyclonedds_cpp'
    rows = snapshot_worker('writer', rmw=rmw, count=0)
    observation = next(row['participant_ownership_observation'] for row in rows
                       if row['event'] == 'participant_ownership_snapshot')
    assert observation['complete'] is False
    assert observation['candidate_eligible'] is False
    assert observation['reason'] == {
        'missing_api': 'participant_ownership_api_unavailable:dds_get_parent',
        'capture_error': 'fixture_ownership_read_failed',
        'fastdds_blackbox': 'rmw_participant_ownership_observer_unavailable'}[case]
    snapshot = next(row for row in rows if row['event'] == 'match_snapshot')
    assert snapshot['matched_count'] == 0
    if case != 'fastdds_blackbox':
        assert snapshot['peer_gids_complete'] is True
    assert any(row['event'] == 'command_done' and row['command_id'] == 1 for row in rows)
    assert not any(row.get('observation_mode') == 'instrumentation' for row in rows)


def test_configured_group_stays_declaration_only_after_actual_ownership_observation(native):
    spec = dict(logical_id='w0', participant_id='configured_shared_group', role='writer',
                topic='/fixture', type_name='std_msgs/msg/String', qos=dict(
                    reliability='reliable', durability='volatile', history='keep_last', depth=1,
                    liveliness='automatic', deadline_sec=10.0, lease_duration_sec=10.0,
                    ownership='shared', partitions=[]))
    before = worker.endpoint_expressibility(spec, {})['binding']
    observed = native.sample_ownership(196, ENDPOINT, 'writer')
    after = worker.endpoint_expressibility(spec, {})['binding']
    assert before == after
    assert after['participant_id_status'] == 'declaration_only'
    assert after['execution_evidence'] is False
    assert observed['participant_gid'] == PARTICIPANT
    assert 'configured_shared_group' not in str(observed)


def test_equal_guid_prefix_cannot_replace_missing_ownership_api(native):
    native.lib.guids[10] = ENDPOINT[:24] + '000001c1'
    native.lib.dds_get_participant = None
    receipt = native.sample_ownership(196, ENDPOINT, 'writer')
    assert receipt['complete'] is False
    assert 'participant_gid' not in receipt


def test_pinned_0105_domain_parent_is_corroborated_without_becoming_participant(native):
    # Pinned 0.10.5 dds_participant.c:152 attaches participant to domain.
    # dds_entity.c:577/605/1243 exposes these relations through public APIs.
    native.lib.parents.update({10: 90, 90: 99, 99: 0})
    native.lib.children[90] = [10]
    native.lib.domains[90] = 196
    native.lib.dds_get_participant = lambda handle: 0 if handle in (90, 99) else 10
    receipt = native.sample_ownership(196, ENDPOINT, 'writer')
    assert receipt.get('reason') is None
    assert receipt['complete'] is True, receipt
    assert receipt['participant_handle'] == 10
    assert receipt['participant_gid'] == PARTICIPANT
    assert receipt['parent_chain'] == [30, 20, 10]
    assert receipt['candidate_eligible'] is False
    assert receipt['root_diagnostics'][0]['participant_parent_return'] == 90
    assert receipt['root_diagnostics'][0]['parent_domain_id'] == 196
    assert receipt['root_diagnostics'][0]['parent_participant_return'] == 0


def test_failed_root_children_api_retains_actual_error_return(native):
    native.lib.parents[10] = 90
    native.lib.domains[90] = 196
    native.lib.dds_get_participant = lambda handle: 0 if handle == 90 else 10
    original = native.lib.dds_get_children
    native.lib.dds_get_children = lambda handle, array, size: (
        -9 if handle == 90 else original(handle, array, size))
    receipt = native.sample_ownership(196, ENDPOINT, 'writer')
    assert receipt['complete'] is False
    assert 'participant_gid' not in receipt
    assert receipt['reason'] == 'native_enumeration_error_or_capacity_exceeded'
    root = receipt['root_diagnostics'][0]
    assert root['participant_parent_return'] == 90
    assert root['parent_domain_return_code'] == 0
    assert root['parent_children_queries'] == [{'capacity': 512, 'return_code': -9}]


@pytest.mark.parametrize('damage', [
    'negative_parent', 'self_parent', 'endpoint_parent', 'publisher_parent',
    'foreign_domain', 'domain_api_error', 'parent_has_participant',
    'parent_participant_error', 'missing_reverse_child', 'parent_cycle',
    'ancestor_cycle', 'ancestor_endpoint', 'ancestor_error', 'ancestry_capacity',
    'participant_not_self', 'root_lookup_lost', 'duplicate_parent_children',
    'root_changed_between_samples',
])
def test_unverified_positive_roots_remain_unknown_and_supporting(native, damage):
    lib = native.lib
    lib.parents.update({10: 90, 90: 99, 99: 0})
    lib.children[90] = [10]
    lib.domains[90] = 196
    lib.dds_get_participant = lambda handle: 0 if handle in (90, 99) else 10
    if damage == 'negative_parent': lib.parents[10] = -9
    elif damage == 'self_parent': lib.parents[10] = 10
    elif damage == 'endpoint_parent': lib.parents[10] = 30
    elif damage == 'publisher_parent': lib.parents[10] = 20
    elif damage == 'foreign_domain': lib.domains[90] = 197
    elif damage == 'domain_api_error':
        original = lib.dds_get_domainid
        lib.dds_get_domainid = lambda handle, ptr: -9 if handle == 90 else original(handle, ptr)
    elif damage in ('parent_has_participant', 'parent_participant_error'):
        lib.dds_get_participant = lambda handle: (
            (40 if damage == 'parent_has_participant' else -9) if handle == 90 else 10)
    elif damage == 'missing_reverse_child': lib.children[90] = []
    elif damage == 'duplicate_parent_children': lib.children[90] = [10, 10]
    elif damage == 'parent_cycle': lib.parents[90] = 90
    elif damage == 'ancestor_cycle': lib.parents[99] = 90
    elif damage == 'ancestor_endpoint': lib.parents[99] = 30
    elif damage == 'ancestor_error': lib.parents[99] = -9
    elif damage == 'ancestry_capacity':
        for handle in range(99, 99 + native.MAX_ENTITIES):
            lib.parents[handle] = handle + 1
    elif damage == 'participant_not_self':
        lib.dds_get_participant = lambda handle: 40 if handle == 10 else 10
    elif damage == 'root_lookup_lost':
        calls = 0
        original = lib.dds_lookup_participant
        def lookup(*args):
            nonlocal calls
            calls += 1
            return 0 if calls > 1 else original(*args)
        lib.dds_lookup_participant = lookup
    elif damage == 'root_changed_between_samples':
        calls = 0
        original = lib.dds_get_parent
        def parent(handle):
            nonlocal calls
            if handle == 10:
                calls += 1
                if calls == 2:
                    return 0  # both root forms valid individually, unstable together
            return original(handle)
        lib.dds_get_parent = parent
    receipt = native.sample_ownership(196, ENDPOINT, 'reader')
    assert receipt['complete'] is False
    assert receipt['status'] == 'unknown'
    assert receipt['candidate_eligible'] is False
    assert 'participant_gid' not in receipt
    assert receipt['reason'] == {
        'duplicate_parent_children': 'native_handles_invalid_or_aliased',
        'root_changed_between_samples': 'participant_ownership_inventory_changed',
    }.get(damage, 'participant_root_invalid')
    root = receipt['root_diagnostics'][0]
    assert root['participant_parent_return'] == {
        'negative_parent': -9, 'self_parent': 10, 'endpoint_parent': 30,
        'publisher_parent': 20,
    }.get(damage, 90)
    if damage == 'domain_api_error':
        assert root['parent_domain_return_code'] == -9
        assert root['parent_domain_id'] is None
    if damage == 'parent_participant_error':
        assert root['parent_participant_return'] == -9
    if damage == 'ancestor_error':
        assert root['parent_ancestry'][-1] == [99, -9]

@pytest.mark.parametrize('failure', ['zero', 'error', 'duplicate'])
def test_failed_inventory_retains_bounded_handle_guid_diagnostics(native, failure):
    original = native.lib.dds_get_guid
    participant_reads = 0
    def read(handle, ptr):
        nonlocal participant_reads
        if handle == 10:
            participant_reads += 1
            if participant_reads == 2:
                if failure == 'error':
                    return -7  # output buffer deliberately left untouched
                ptr._obj.v[:] = bytes.fromhex('0' * 32 if failure == 'zero' else ENDPOINT)
                return 0
        return original(handle, ptr)
    native.lib.dds_get_guid = read

    receipt = native.sample_ownership(196, ENDPOINT, 'writer')

    assert receipt['complete'] is False
    assert receipt['candidate_eligible'] is False
    assert receipt['reason'] == 'participant_inventory_guid_invalid_or_aliased'
    assert 'participant_gid' not in receipt
    assert 'local_handle' not in receipt and 'participant_handle' not in receipt
    assert len(receipt['inventory_diagnostics']) == 1
    capture = receipt['inventory_diagnostics'][0]
    assert capture['entity_inventory'] == [[10, [20]], [20, [30]], [30, []]]
    assert len(capture['entity_identities']) <= native.MAX_ENTITIES
    failed = next(row for row in capture['entity_identities'] if row['handle'] == 10)
    assert failed['guid_return_code'] == (-7 if failure == 'error' else 0)
    assert failed['guid'] == (None if failure == 'error' else
                              '0' * 32 if failure == 'zero' else ENDPOINT)
    assert failed['observed_domain_id'] == 196
    assert failed['domain_return_code'] == 0
    assert capture['failure_category'] == 'participant_inventory_guid_invalid_or_aliased'
    assert failed['known_labels'] == ['participant']

@pytest.mark.parametrize('capture_number', [1, 2])
@pytest.mark.parametrize('failure', ['zero', 'error', 'duplicate'])
def test_unknown_inventory_handle_preserves_only_observed_values(native, capture_number, failure):
    original = native.lib.dds_get_guid
    reads = 0
    def read(handle, ptr):
        nonlocal reads
        if handle == 20:
            reads += 1
            # Each capture locates first, then independently reads the inventory.
            if reads == capture_number * 2:
                ptr._obj.v[:] = bytes.fromhex(PARTICIPANT if failure == 'duplicate' else '0' * 32)
                return -7 if failure == 'error' else 0
        return original(handle, ptr)
    native.lib.dds_get_guid = read
    receipt = native.sample_ownership(196, ENDPOINT, 'reader')
    assert receipt['complete'] is False and receipt['candidate_eligible'] is False
    assert receipt['reason'] == 'participant_inventory_guid_invalid_or_aliased'
    assert not {'participant_gid', 'participant_handle', 'local_handle', 'entity_identities',
                'entity_inventory', 'parent_chain', 'participant_root_observation'} & receipt.keys()
    captures = receipt['inventory_diagnostics']
    assert len(captures) == capture_number
    assert all(len(row['entity_identities']) <= native.MAX_ENTITIES for row in captures)
    assert captures[-1]['failure_category'] == receipt['reason']
    diagnostic = captures[-1]['entity_identities'][1]
    assert diagnostic == dict(handle=20, known_labels=[], domain_return_code=0,
                             observed_domain_id=196, guid_return_code=-7 if failure == 'error' else 0,
                             guid=None if failure == 'error' else
                             PARTICIPANT if failure == 'duplicate' else '0' * 32)
    if capture_number == 2:
        assert 'failure_category' not in captures[0]
        assert captures[0]['entity_identities'][1]['guid_return_code'] == -12
        assert captures[0]['entity_identities'][1]['guid'] is None


@pytest.mark.parametrize('domain_rc,observed_domain', [(-9, 196), (0, 197)])
def test_inventory_domain_failure_retains_rc_and_safe_observation(native, domain_rc, observed_domain):
    original = native.lib.dds_get_domainid
    def read(handle, ptr):
        if handle == 20:
            ptr._obj.value = observed_domain
            return domain_rc
        return original(handle, ptr)
    native.lib.dds_get_domainid = read
    receipt = native.sample_ownership(196, ENDPOINT, 'writer')
    assert receipt['complete'] is False and receipt['candidate_eligible'] is False
    assert receipt['reason'] == 'participant_inventory_domain_mismatch'
    capture = receipt['inventory_diagnostics'][0]
    assert capture['failure_category'] == receipt['reason']
    assert capture['entity_identities'][-1] == dict(
        handle=20, known_labels=[], domain_return_code=domain_rc,
        observed_domain_id=observed_domain if domain_rc == 0 else None,
        guid_return_code=None, guid=None)
    assert 'participant_gid' not in receipt


def test_capture_diagnostics_reach_exact_entity_bound_without_changing_success(native):
    native.MAX_ENTITIES = 3
    receipt = native.sample_ownership(196, ENDPOINT, 'writer')
    assert receipt['complete'] is True
    assert receipt['candidate_eligible'] is False
    assert len(receipt['inventory_diagnostics']) == 2
    for capture in receipt['inventory_diagnostics']:
        assert len(capture['entity_identities']) == native.MAX_ENTITIES
        assert 'failure_category' not in capture
        assert capture['entity_identities'][0]['known_labels'] == ['participant']
        assert capture['entity_identities'][2]['known_labels'] == ['local_endpoint']


def test_inventory_api_exception_is_retained_without_fabricated_guid(native):
    original = native.lib.dds_get_guid
    reads = 0
    def read(handle, ptr):
        nonlocal reads
        if handle == 20:
            reads += 1
            if reads == 2:
                raise OSError('fixture_inventory_guid_read_failed')
        return original(handle, ptr)
    native.lib.dds_get_guid = read
    receipt = native.sample_ownership(196, ENDPOINT, 'writer')
    assert receipt['complete'] is False and receipt['candidate_eligible'] is False
    assert receipt['reason'] == 'fixture_inventory_guid_read_failed'
    capture = receipt['inventory_diagnostics'][0]
    assert capture['failure_category'] == receipt['reason']
    assert capture['entity_identities'][-1]['guid_return_code'] is None
    assert capture['entity_identities'][-1]['guid'] is None
    assert 'participant_gid' not in receipt


@pytest.mark.parametrize('kind', ['writer', 'reader'])
def test_failed_inventory_diagnostics_survive_worker_raw_envelope(
        native, monkeypatch, worker_guards, snapshot_worker, kind):
    native.lib.guids[30] = '1' * 32 if kind == 'writer' else '2' * 32
    original = native.lib.dds_get_guid
    def read(handle, ptr):
        if handle == 20:
            ptr._obj.v[:] = bytes.fromhex('f' * 32)  # error output is never evidence
            return -7
        return original(handle, ptr)
    native.lib.dds_get_guid = read
    native.lib.dds_get_matched_subscription_data = lambda *args: None
    native.lib.dds_get_matched_publication_data = lambda *args: None
    monkeypatch.setattr(worker, 'CycloneMatchedPeerObserver', lambda: native)
    rows = snapshot_worker(kind, rmw='rmw_cyclonedds_cpp', count=0)
    raw = next(row for row in rows if row['event'] == 'participant_ownership_snapshot')
    receipt = raw['participant_ownership_observation']
    assert raw['candidate_eligible'] is False and raw['supporting_diagnostic'] is True
    assert receipt['complete'] is False and receipt['candidate_eligible'] is False
    assert receipt['reason'] == 'participant_inventory_guid_invalid_or_aliased'
    assert receipt['inventory_diagnostics'][0]['entity_identities'][-1] == dict(
        handle=20, known_labels=[], domain_return_code=0, observed_domain_id=196,
        guid_return_code=-7, guid=None)
    assert 'participant_gid' not in receipt and 'participant_handle' not in receipt
    assert any(row['event'] == 'command_done' and row['command_id'] == raw['command_id']
               for row in rows)


def topic_probe_symbols(native, name=b'offline_topic', type_name=b'offline::Type'):
    """Authored public API readbacks; no native library or DDS execution."""
    def probe(payload):
        def read(handle, buffer, capacity):
            if handle != 20:
                buffer[0] = b'\x00'
                return -12
            copied = payload[:capacity - 1]
            buffer[:len(copied)] = copied
            buffer[len(copied)] = b'\x00'
            return len(payload)
        return read
    native.lib.dds_get_name = probe(name)
    native.lib.dds_get_type_name = probe(type_name)


def test_topic_api_evidence_precedes_zero_guid_rejection_without_qualification(native):
    topic_probe_symbols(native)
    original = native.lib.dds_get_guid
    def guid(handle, ptr):
        if handle == 20:
            ptr._obj.v[:] = bytes(16)
            return 0
        return original(handle, ptr)
    native.lib.dds_get_guid = guid
    receipt = native.sample_ownership(196, ENDPOINT, 'writer')
    assert receipt['status'] == 'unknown' and receipt['complete'] is False
    assert receipt['candidate_eligible'] is False
    assert receipt['reason'] == 'participant_inventory_guid_invalid_or_aliased'
    assert 'participant_gid' not in receipt and 'participant_handle' not in receipt
    capture = receipt['inventory_diagnostics'][0]
    assert capture['failure_category'] == receipt['reason']
    assert [r['handle'] for r in capture['topic_kind_diagnostics']] == [10, 20]
    failed = capture['entity_identities'][-1]
    assert failed['guid_return_code'] == 0 and failed['guid'] == '0' * 32
    assert failed['known_labels'] == ['topic']
    evidence = capture['topic_kind_diagnostics'][-1]
    assert evidence['kind'] == 'topic'
    assert evidence['probes']['dds_get_name']['return_code'] == 13
    assert evidence['probes']['dds_get_name']['value'] == 'offline_topic'
    assert evidence['probes']['dds_get_type_name']['return_code'] == 13
    assert evidence['probes']['dds_get_type_name']['value'] == 'offline::Type'
    assert all(p['status'] == 'observed' for p in evidence['probes'].values())


@pytest.mark.parametrize('api', ['dds_get_name', 'dds_get_type_name'])
def test_unsupported_topic_probe_is_explicit_and_other_probe_survives(native, api):
    topic_probe_symbols(native)
    def unsupported(handle, buffer, capacity):
        buffer[:4] = b'bad\x00'
        return -12
    setattr(native.lib, api, unsupported)
    receipt = native.sample_ownership(196, ENDPOINT, 'writer')
    assert receipt['complete'] is True and receipt['candidate_eligible'] is False
    evidence = receipt['inventory_diagnostics'][0]['topic_kind_diagnostics'][1]
    assert evidence['kind'] == 'unknown'
    probe = evidence['probes'][api]
    assert probe['status'] == 'unsupported'
    assert probe['return_code'] == -12 and probe['value'] is None
    assert probe['buffer_hex'].startswith('62616400')
    other = 'dds_get_type_name' if api == 'dds_get_name' else 'dds_get_name'
    assert evidence['probes'][other]['status'] == 'observed'


@pytest.mark.parametrize('api', ['dds_get_name', 'dds_get_type_name'])
@pytest.mark.parametrize('case,status,error,rc', [
    ('absent', 'unavailable', 'optional_symbol_absent', None),
    ('error', 'error', 'api_error', -7),
    ('exception', 'error', 'optional_probe_exception', None),
    ('truncated', 'truncated', 'reported_length_exceeds_buffer', 400),
    ('boundary', 'truncated', 'reported_length_exceeds_buffer', 256),
    ('invalid_utf8', 'malformed', 'invalid_utf8', 1),
    ('no_terminator', 'malformed', 'length_or_terminator_mismatch', 3),
    ('embedded_nul', 'malformed', 'length_or_terminator_mismatch', 3),
    ('empty', 'malformed', 'empty_name', 0),
    ('bad_rc', 'malformed', 'return_code_not_integer', True),
])
def test_optional_topic_probe_failures_are_bounded_independent_and_non_authoritative(
        native, api, case, status, error, rc):
    topic_probe_symbols(native)
    def damaged(handle, buffer, capacity):
        if case == 'exception':
            buffer[:4] = b'bad\x00'
            raise OSError('offline_probe_failure')
        payload = {'invalid_utf8': b'\xff\x00', 'no_terminator': b'abcX',
                   'embedded_nul': b'a\x00b\x00', 'empty': b'\x00'}.get(case, b'bad\x00')
        buffer[:len(payload)] = payload
        return rc
    if case == 'absent':
        delattr(native.lib, api)
    else:
        setattr(native.lib, api, damaged)
    receipt = native.sample_ownership(196, ENDPOINT, 'writer')
    assert receipt['complete'] is True and receipt['participant_gid'] == PARTICIPANT
    assert receipt['candidate_eligible'] is False
    assert len(receipt['inventory_diagnostics']) == 2
    for capture in receipt['inventory_diagnostics']:
        assert len(capture['topic_kind_diagnostics']) == 3
        evidence = capture['topic_kind_diagnostics'][1]
        assert evidence['kind'] == 'unknown'
        assert capture['entity_identities'][1]['known_labels'] == []
        probe = evidence['probes'][api]
        assert probe['status'] == status and probe['error'] == error
        assert probe['return_code'] == rc and probe['value'] is None
        assert probe['capacity_bytes'] == 256
        if case == 'absent':
            assert probe['buffer_hex'] is None
        else:
            assert len(bytes.fromhex(probe['buffer_hex'])) == 256
        other = 'dds_get_type_name' if api == 'dds_get_name' else 'dds_get_name'
        assert evidence['probes'][other]['status'] == 'observed'


@pytest.mark.parametrize('guid_case', ['zero', 'error', 'changed_nonzero'])
def test_topic_observation_never_qualifies_invalid_local_endpoint_guid(native, guid_case):
    topic_probe_symbols(native)
    original = native.lib.dds_get_guid
    reads = 0
    def read(handle, ptr):
        nonlocal reads
        if handle == 30:
            reads += 1
            if reads == 2:
                ptr._obj.v[:] = bytes.fromhex('0' * 32 if guid_case == 'zero' else 'b' * 32)
                return -7 if guid_case == 'error' else 0
        return original(handle, ptr)
    native.lib.dds_get_guid = read
    receipt = native.sample_ownership(196, ENDPOINT, 'writer')
    assert receipt['complete'] is False and receipt['status'] == 'unknown'
    assert receipt['candidate_eligible'] is False and 'participant_gid' not in receipt
    expected = ('participant_endpoint_guid_changed' if guid_case == 'changed_nonzero'
                else 'participant_inventory_guid_invalid_or_aliased')
    assert receipt['reason'] == expected
    capture = receipt['inventory_diagnostics'][0]
    assert capture['failure_category'] == expected
    assert [r['handle'] for r in capture['topic_kind_diagnostics']] == [10, 20, 30]


def test_absent_topic_symbols_preserve_matched_peer_observer(native):
    native.lib.dds_get_matched_subscription_data = lambda *args: None
    receipt = native.sample(196, ENDPOINT, 'writer')
    assert receipt['complete'] is True
    assert receipt['matched_peer_gids'] == []


@pytest.mark.parametrize('kind', ['writer', 'reader'])
def test_topic_diagnostic_survives_primary_guid_exception_in_worker_raw(
        native, monkeypatch, worker_guards, snapshot_worker, kind):
    topic_probe_symbols(native)
    native.lib.guids[30] = '1' * 32 if kind == 'writer' else '2' * 32
    original = native.lib.dds_get_guid
    name_probe = native.lib.dds_get_name
    probed = set()
    def name_read(handle, buffer, capacity):
        probed.add(handle)
        return name_probe(handle, buffer, capacity)
    native.lib.dds_get_name = name_read
    def read(handle, ptr):
        if handle == 20 and handle in probed:
            raise OSError('offline_primary_guid_failure')
        return original(handle, ptr)
    native.lib.dds_get_guid = read
    native.lib.dds_get_matched_subscription_data = lambda *args: None
    native.lib.dds_get_matched_publication_data = lambda *args: None
    monkeypatch.setattr(worker, 'CycloneMatchedPeerObserver', lambda: native)
    rows = snapshot_worker(kind, rmw='rmw_cyclonedds_cpp', count=0)
    raw = next(row for row in rows if row['event'] == 'participant_ownership_snapshot')
    receipt = raw['participant_ownership_observation']
    assert receipt['reason'] == 'offline_primary_guid_failure'
    assert receipt['status'] == 'unknown' and receipt['complete'] is False
    assert raw['candidate_eligible'] is False and receipt['candidate_eligible'] is False
    assert 'participant_gid' not in receipt
    capture = receipt['inventory_diagnostics'][0]
    assert capture['topic_kind_diagnostics'][-1]['kind'] == 'topic'
    assert capture['entity_identities'][-1]['guid_return_code'] is None
    assert capture['failure_category'] == 'offline_primary_guid_failure'
    assert any(row['event'] == 'command_done' for row in rows)


def test_optional_binding_failure_cannot_break_ownership_or_other_readback(native):
    topic_probe_symbols(native)
    class Unbindable:
        def __setattr__(self, name, value):
            raise RuntimeError('offline_binding_failure')
    native.lib.dds_get_name = Unbindable()
    receipt = native.sample_ownership(196, ENDPOINT, 'writer')
    assert receipt['complete'] is True
    evidence = receipt['inventory_diagnostics'][0]['topic_kind_diagnostics'][1]
    assert evidence['kind'] == 'unknown'
    assert evidence['probes']['dds_get_name']['error'] == 'optional_probe_exception'
    assert evidence['probes']['dds_get_name']['return_code'] is None
    assert evidence['probes']['dds_get_type_name']['value'] == 'offline::Type'


def test_optional_success_keeps_canonical_ownership_exactly_equal(native):
    before = native.sample_ownership(196, ENDPOINT, 'writer')
    topic_probe_symbols(native)
    after = native.sample_ownership(196, ENDPOINT, 'writer')
    for key in ('complete', 'status', 'candidate_eligible', 'local_handle',
                'participant_handle', 'participant_gid', 'parent_chain',
                'entity_inventory', 'entity_identities', 'participant_root_observation',
                'stable_sample_count', 'ownership_api', 'lookup_api'):
        assert after[key] == before[key]
    probe = after['inventory_diagnostics'][0]['topic_kind_diagnostics'][1]
    assert probe['kind'] == 'topic'
    assert probe['probes']['dds_get_name']['buffer_hex'].startswith('6f66666c696e655f746f70696300')


def test_domain_failure_retains_kind_probes_before_primary_validation(native):
    topic_probe_symbols(native)
    native.lib.domains[20] = 197
    receipt = native.sample_ownership(196, ENDPOINT, 'writer')
    assert receipt['complete'] is False and receipt['status'] == 'unknown'
    assert receipt['reason'] == 'participant_inventory_domain_mismatch'
    capture = receipt['inventory_diagnostics'][0]
    assert capture['topic_kind_diagnostics'][-1]['kind'] == 'topic'
    assert capture['entity_identities'][-1]['guid_return_code'] is None
    assert capture['failure_category'] == receipt['reason']


def test_full_bounded_utf8_copy_is_retained_without_guid_gate_change(native):
    topic_probe_symbols(native, name=b'a' * 255, type_name='offline::类型'.encode('utf-8'))
    receipt = native.sample_ownership(196, ENDPOINT, 'writer')
    assert receipt['complete'] is True
    evidence = receipt['inventory_diagnostics'][0]['topic_kind_diagnostics'][1]
    assert evidence['kind'] == 'topic'
    name = evidence['probes']['dds_get_name']
    assert name['return_code'] == 255 and name['value'] == 'a' * 255
    assert name['buffer_hex'] == '61' * 255 + '00'
    typename = evidence['probes']['dds_get_type_name']
    assert typename['return_code'] == 15 and typename['value'] == 'offline::类型'
    assert receipt['participant_gid'] == PARTICIPANT and receipt['candidate_eligible'] is False


@pytest.mark.parametrize('formatting_error', [RuntimeError, KeyboardInterrupt])
@pytest.mark.parametrize('guid_failure', [False, True])
@pytest.mark.parametrize('api', ['dds_get_name', 'dds_get_type_name'])
def test_unprintable_optional_exception_preserves_ownership_outcome(
        native, api, guid_failure, formatting_error):
    topic_probe_symbols(native)
    class UnprintableProbeError(Exception):
        def __str__(self):
            raise formatting_error('offline_exception_formatting_failed')
    def broken(handle, buffer, capacity):
        buffer[:4] = b'bad\x00'
        raise UnprintableProbeError()
    setattr(native.lib, api, broken)
    if guid_failure:
        original = native.lib.dds_get_guid
        def zero(handle, ptr):
            if handle == 20:
                ptr._obj.v[:] = bytes(16)
                return 0
            return original(handle, ptr)
        native.lib.dds_get_guid = zero
    receipt = native.sample_ownership(196, ENDPOINT, 'writer')
    if guid_failure:
        assert receipt['reason'] == 'participant_inventory_guid_invalid_or_aliased'
        assert receipt['complete'] is False and receipt['status'] == 'unknown'
        assert 'participant_gid' not in receipt
        capture = receipt['inventory_diagnostics'][0]
        assert capture['failure_category'] == receipt['reason']
        assert capture['entity_identities'][-1]['guid_return_code'] == 0
        assert capture['entity_identities'][-1]['guid'] == '0' * 32
    else:
        assert receipt['complete'] is True and receipt['status'] == 'observed'
        assert receipt['participant_gid'] == PARTICIPANT
        assert receipt['stable_sample_count'] == 2
    assert receipt['candidate_eligible'] is False
    for capture in receipt['inventory_diagnostics']:
        for evidence in capture['topic_kind_diagnostics']:
            assert evidence['kind'] == 'unknown'
            probe = evidence['probes'][api]
            assert probe['error'] == 'optional_probe_exception'
            assert probe['exception_type'] == 'UnprintableProbeError'
            assert probe['detail'] == '<exception_text_unavailable>'
            assert len(probe['detail']) <= 256
            assert probe['buffer_hex'].startswith('62616400')
            assert len(bytes.fromhex(probe['buffer_hex'])) == 256
            assert probe['return_code'] is None
        other = 'dds_get_type_name' if api == 'dds_get_name' else 'dds_get_name'
        assert capture['topic_kind_diagnostics'][1]['probes'][other]['status'] == 'observed'
