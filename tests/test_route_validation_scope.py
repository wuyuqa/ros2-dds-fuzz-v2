"""Context-local route authentication. Pure offline tests; no runtime dispatch."""
import asyncio
from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
import json
from pathlib import Path
import threading

import pytest

from r2f.campaign import mutation_routes as routes
from r2f.campaign.corpus import content_identity, corpus_from_profile
from r2f.modules.s1 import targets

ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture
def entry():
    profile = json.loads((ROOT / 'config/s1_fuzz/profiles/common-baseline-routes.json').read_text())
    return corpus_from_profile(profile, cycle_id='common-baseline').selectable()[0]


@pytest.fixture
def counted(monkeypatch):
    factory = targets.exploratory_target_declarations
    calls = []

    def build(*args, **kwargs):
        result = factory(*args, **kwargs)
        calls.append(result)
        return result

    monkeypatch.setattr(targets, 'exploratory_target_declarations', build)
    return calls


def test_cold_calls_rebuild_and_scope_builds_once(entry, counted):
    routes.validate_route_entry(entry)
    routes.validate_route_entry(entry)
    assert len(counted) == 2
    with routes._route_validation_scope() as exposed:
        assert exposed is None
        for _ in range(4):
            routes.validate_route_entry(entry)
    assert len(counted) == 3
    routes.validate_route_entry(entry)
    assert len(counted) == 4
    assert routes._ROUTE_VALIDATION_TARGETS.get() is None


def test_snapshot_does_not_alias_mutable_models(entry, counted):
    with routes._route_validation_scope():
        routes.validate_route_entry(entry)
        projection = routes._ROUTE_VALIDATION_TARGETS.get()
        target = projection[entry['target_id']]
        with pytest.raises(TypeError):
            projection[entry['target_id']] = target
        with pytest.raises(TypeError):
            target.surfaces['new'] = None
        model = counted[-1][entry['target_id']]
        model.target_revision += 1
        surface = model.exploration_contract.surface_by_id[entry['mutation_routes'][0]['surface_id']]
        surface.boundary_classes.clear()
        surface.mutator_ids.clear()
        routes.validate_route_entry(entry)
    assert routes._ROUTE_VALIDATION_TARGETS.get() is None


def test_new_scope_sees_changed_model_and_monkeypatch(entry, counted, monkeypatch):
    with routes._route_validation_scope():
        routes.validate_route_entry(entry)
    changed = counted[0]
    changed[entry['target_id']].target_revision += 1
    monkeypatch.setattr(targets, 'exploratory_target_declarations', lambda: changed)
    with routes._route_validation_scope():
        with pytest.raises(ValueError, match='revision/contract'):
            routes.validate_route_entry(entry)
    with pytest.raises(ValueError, match='revision/contract'):
        routes.validate_route_entry(entry)


def test_nested_scope_is_fresh_and_restores_parent_on_error(entry, counted, monkeypatch):
    with routes._route_validation_scope():
        routes.validate_route_entry(entry)
        parent = routes._ROUTE_VALIDATION_TARGETS.get()
        changed = counted[-1]
        changed[entry['target_id']].target_revision += 1
        with monkeypatch.context() as patch:
            patch.setattr(targets, 'exploratory_target_declarations', lambda: changed)
            with pytest.raises(ValueError, match='revision/contract'):
                with routes._route_validation_scope():
                    assert routes._ROUTE_VALIDATION_TARGETS.get() is not parent
                    routes.validate_route_entry(entry)
        assert routes._ROUTE_VALIDATION_TARGETS.get() is parent
        routes.validate_route_entry(entry)
    assert routes._ROUTE_VALIDATION_TARGETS.get() is None


def test_failed_snapshot_construction_keeps_parent(entry, monkeypatch):
    with routes._route_validation_scope():
        routes.validate_route_entry(entry)
        parent = routes._ROUTE_VALIDATION_TARGETS.get()
        def fail():
            raise RuntimeError('construction failed')
        monkeypatch.setattr(targets, 'exploratory_target_declarations', fail)
        with pytest.raises(RuntimeError, match='construction failed'):
            with routes._route_validation_scope():
                routes.validate_route_entry(entry)
        assert routes._ROUTE_VALIDATION_TARGETS.get() is parent
        routes.validate_route_entry(entry)
    assert routes._ROUTE_VALIDATION_TARGETS.get() is None


def test_thread_scopes_do_not_share_revision(entry, monkeypatch):
    factory = targets.exploratory_target_declarations
    local = threading.local()
    barrier = threading.Barrier(2)
    def build():
        result = factory()
        result[entry['target_id']].target_revision += local.offset
        return result
    monkeypatch.setattr(targets, 'exploratory_target_declarations', build)
    def work(offset):
        local.offset = offset
        own = deepcopy(entry)
        own['target_revision'] += offset
        for origin in own['mutation_route_origins']:
            origin['baseline_content_sha256'] = content_identity(own)
        with routes._route_validation_scope():
            barrier.wait(timeout=10)
            routes.validate_route_entry(own)
            if offset:
                with pytest.raises(ValueError, match='revision/contract'):
                    routes.validate_route_entry(entry)
        assert routes._ROUTE_VALIDATION_TARGETS.get() is None
    with ThreadPoolExecutor(max_workers=2) as pool:
        list(pool.map(work, [0, 1]))


def test_async_nested_scopes_do_not_overwrite_inherited_parent(entry, monkeypatch):
    factory = targets.exploratory_target_declarations
    def build():
        result = factory()
        task = asyncio.current_task()
        offset = int(task.get_name()) if task and task.get_name() in ('0', '1') else 0
        result[entry['target_id']].target_revision += offset
        return result
    async def check():
        monkeypatch.setattr(targets, 'exploratory_target_declarations', build)
        with routes._route_validation_scope():
            routes.validate_route_entry(entry)
            parent = routes._ROUTE_VALIDATION_TARGETS.get()
            async def work(offset):
                assert routes._ROUTE_VALIDATION_TARGETS.get() is parent
                own = deepcopy(entry)
                own['target_revision'] += offset
                for origin in own['mutation_route_origins']:
                    origin['baseline_content_sha256'] = content_identity(own)
                with routes._route_validation_scope():
                    await asyncio.sleep(0)
                    routes.validate_route_entry(own)
                assert routes._ROUTE_VALIDATION_TARGETS.get() is parent
            await asyncio.gather(*(asyncio.create_task(work(i), name=str(i)) for i in (0, 1)))
            routes.validate_route_entry(entry)
    asyncio.run(check())
    assert routes._ROUTE_VALIDATION_TARGETS.get() is None


@pytest.mark.parametrize('damage', ['revision', 'hash', 'surface', 'boundary', 'mutator', 'capability'])
def test_scoped_validation_still_rejects_each_record(entry, damage):
    bad = deepcopy(entry)
    if damage == 'revision':
        bad['target_revision'] += 1
    elif damage == 'hash':
        bad['contract_hash'] = '0' * 64
    elif damage == 'capability':
        bad['runtime_capability'] = 'caller-trusted'
    else:
        field = {'surface': 'surface_id', 'boundary': 'boundary_class', 'mutator': 'mutator_id'}[damage]
        bad['mutation_routes'][0][field] = 'not-authorized'
    bad.pop('mutation_route_origins', None)
    with routes._route_validation_scope():
        routes.validate_route_entry(entry)
        with pytest.raises(ValueError):
            routes.validate_route_entry(bad)
        routes.validate_route_entry(entry)


def test_caller_contract_is_not_authority(entry):
    model = targets.exploratory_target_declarations()[entry['target_id']]
    contract = model.exploration_contract
    contract.contract_version += 1
    bad = deepcopy(entry)
    bad['contract_hash'] = contract.contract_hash()
    for origin in bad['mutation_route_origins']:
        origin['baseline_content_sha256'] = content_identity(bad)
    with routes._route_validation_scope():
        with pytest.raises(ValueError, match='revision/contract'):
            routes.validate_route_entry(bad, contract=contract)
        with pytest.raises(ValueError, match='revision/contract'):
            routes.validate_route_entry(entry, contract=contract)


def test_checkpoint_transaction_builds_exactly_one_directory(tmp_path, monkeypatch, counted):
    from r2f.campaign import checkpoint
    from test_model_search_integration import profile, run

    validate = checkpoint._validate_state
    observations = []
    def measured(*args, **kwargs):
        before = len(counted)
        try:
            return validate(*args, **kwargs)
        finally:
            observations.append(len(counted) - before)
            assert routes._ROUTE_VALIDATION_TARGETS.get() is None
    monkeypatch.setattr(checkpoint, '_validate_state', measured)
    result = run(tmp_path / 'scope-count', profile(), maximum=2)
    assert result['budget']['actual'] == 0
    assert len(observations) == 5  # planned/recorded for each input, then final
    assert observations == [1] * 5


def test_unused_and_non_routed_scopes_never_build_directory(counted):
    with routes._route_validation_scope():
        assert routes.validate_route_entry({}) is None
        with routes._route_validation_scope():
            assert routes.validate_route_entry({}) is None
    assert counted == []
    assert routes._ROUTE_VALIDATION_TARGETS.get() is None


def test_non_routed_checkpoint_builds_no_route_directory(tmp_path, monkeypatch, counted):
    from r2f.campaign import checkpoint
    from test_exploratory_checkpoint import REPO, run_cycle

    validate = checkpoint._validate_state
    observations = []
    def measured(*args, **kwargs):
        before = len(counted)
        try:
            return validate(*args, **kwargs)
        finally:
            observations.append(len(counted) - before)
            assert routes._ROUTE_VALIDATION_TARGETS.get() is None
    monkeypatch.setattr(checkpoint, '_validate_state', measured)
    result = run_cycle(tmp_path / 'legacy-scope', json.loads((REPO / 'config/s1_fuzz/profiles/parameter.json').read_text()), maximum=2)
    assert result['budget']['actual'] == 0
    assert observations and observations == [0] * len(observations)
