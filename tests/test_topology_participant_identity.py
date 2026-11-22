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
