"""Binding real topology Python services offline, without runtime execution."""
import inspect

from r2f.core import Registry
from r2f.modules.s1.module import register_exploratory
from r2f.modules.s1.topology_targets import TOPOLOGY_CAPABILITY, TOPOLOGY_TARGET
from r2f.modules.s1.topology_services import _ServiceDependency


def topology_registry():
    registry = Registry()
    register_exploratory(registry, runtime_capability=TOPOLOGY_CAPABILITY)
    return registry


def test_public_binding_resolves_real_topology_services_once_without_invocation():
    from r2f.campaign.topology_adapter import TopologyFactProducer
    from r2f.modules.s1.topology_oracles import TopologyOracle
    registry = topology_registry()
    legacy = registry.get_services(TOPOLOGY_TARGET)
    binding = registry.bind_services(TOPOLOGY_TARGET)
    assert type(binding.services.oracle) is TopologyOracle
    assert type(binding.services.fact_producer) is TopologyFactProducer
    assert isinstance(legacy.oracle, _ServiceDependency)
    assert legacy.oracle.resolve() is binding.services.oracle
    assert legacy.fact_producer.resolve() is binding.services.fact_producer
    assert registry.bind_services(TOPOLOGY_TARGET).services.oracle is binding.services.oracle
    binding.verify_unchanged()
    component = binding.descriptor['components']['oracle']
    assert component['class']['qualname'] == 'TopologyOracle'
    assert component['callables']['evaluate']['qualname'] == 'TopologyOracle.evaluate'
    assert component['callables']['evaluate']['source_file'] == inspect.getsourcefile(TopologyOracle)
    assert component['dependency']['module'] == 'r2f.modules.s1.topology_oracles'
    assert component['dependency']['symbol'] == 'TopologyOracle'
    assert component['dependency']['method'] == 'evaluate'
    assert registry.get_services(TOPOLOGY_TARGET) is legacy


def test_invocation_preserves_binding_and_replacement_after_invocation_fails(monkeypatch):
    import pytest
    from types import SimpleNamespace
    from r2f.modules.s1 import topology_oracles
    registry = topology_registry()
    binding = registry.bind_services(TOPOLOGY_TARGET)
    binding.verify_unchanged()
    # Real Python oracle call on incomplete offline input, never DDS evidence.
    result = binding.services.oracle.evaluate(SimpleNamespace(target_id=TOPOLOGY_TARGET, arm='mutated'), {})
    assert result['verdict'] == 'unknown'
    binding.verify_unchanged()
    original = topology_oracles.TopologyOracle.evaluate

    def replacement(self, scenario, facts):
        return original(self, scenario, facts)

    monkeypatch.setattr(topology_oracles.TopologyOracle, 'evaluate', replacement)
    with pytest.raises(ValueError, match='service_binding_changed'):
        binding.verify_unchanged()


def test_dependency_class_replacement_is_rejected(monkeypatch):
    import pytest
    from r2f.modules.s1 import topology_oracles
    registry = topology_registry()
    binding = registry.bind_services(TOPOLOGY_TARGET)

    class Replacement(topology_oracles.TopologyOracle):
        pass

    monkeypatch.setattr(topology_oracles, 'TopologyOracle', Replacement)
    with pytest.raises(ValueError, match='dependency_changed'):
        binding.verify_unchanged()


def test_legacy_lazy_calls_still_create_fresh_implementations_after_binding(tmp_path, monkeypatch):
    import importlib
    import sys
    module_path = tmp_path / 'cp3_dependency_fixture.py'
    module_path.write_text('''class Oracle:
    created = 0
    def __init__(self):
        type(self).created += 1
        self.number = type(self).created
    def evaluate(self, scenario, facts):
        return self.number
''')
    monkeypatch.syspath_prepend(str(tmp_path))
    module = importlib.import_module('cp3_dependency_fixture')
    monkeypatch.delitem(sys.modules, 'cp3_dependency_fixture')
    monkeypatch.setitem(sys.modules, 'cp3_dependency_fixture', module)
    dependency = _ServiceDependency(module.__name__, 'Oracle', 'evaluate')
    assert module.Oracle.created == 0
    resolved = dependency.resolve()
    assert dependency.resolve() is resolved
    assert resolved.evaluate(None, None) == 1
    assert dependency.evaluate(None, None) == 2
    assert dependency.evaluate(None, None) == 3
    assert dependency.resolve() is resolved
    assert module.Oracle.created == 3


def test_actual_evaluator_dependency_replacement_is_rejected(monkeypatch):
    import pytest
    from r2f.modules.s1 import topology_oracles
    registry = topology_registry()
    binding = registry.bind_services(TOPOLOGY_TARGET)
    original = topology_oracles.evaluate_topology

    def replacement(scenario, facts):
        return original(scenario, facts)

    monkeypatch.setattr(topology_oracles, 'evaluate_topology', replacement)
    with pytest.raises(ValueError, match='service_binding_changed'):
        binding.verify_unchanged()


def test_dependency_slot_drift_and_absent_or_ambiguous_implementations_fail_closed(monkeypatch):
    import pytest
    registry = topology_registry()
    dependency = registry.get_services(TOPOLOGY_TARGET).oracle
    binding = registry.bind_services(TOPOLOGY_TARGET)
    monkeypatch.setattr(dependency, 'symbol', 'MissingOracle')
    with pytest.raises(ValueError, match='dependency_changed'):
        binding.verify_unchanged()
    monkeypatch.setattr(dependency, 'symbol', 'TopologyOracle')
    binding.verify_unchanged()
    monkeypatch.setattr(dependency, 'callable_dependencies', (
        ('r2f.modules.s1.topology_oracles', 'evaluate_topology'),
        ('r2f.modules.s1.topology_oracles', 'evaluate_topology')))
    with pytest.raises(ValueError, match='ambiguous_dependency'):
        registry.bind_services(TOPOLOGY_TARGET)
    fresh = topology_registry()
    monkeypatch.setattr(fresh.get_services(TOPOLOGY_TARGET).oracle, 'symbol', 'MissingOracle')
    with pytest.raises(ValueError, match='dependency_unavailable'):
        fresh.bind_services(TOPOLOGY_TARGET)


def test_fact_function_dependency_replacement_is_rejected(monkeypatch):
    import pytest
    from r2f.campaign import topology_adapter
    registry = topology_registry()
    binding = registry.bind_services(TOPOLOGY_TARGET)
    monkeypatch.setattr(topology_adapter, 'facts_from_events', lambda *args: {'fixture': True})
    with pytest.raises(ValueError, match='service_binding_changed'):
        binding.verify_unchanged()


def test_explicit_resolve_constructs_once_for_concurrent_binding_callers(tmp_path, monkeypatch):
    import importlib.util
    import sys
    from concurrent.futures import ThreadPoolExecutor
    from threading import Barrier
    path = tmp_path / 'cp3_concurrent_dependency_fixture.py'
    path.write_text('''import time
class Oracle:
    created = 0
    def __init__(self):
        type(self).created += 1
        time.sleep(0.02)
    def evaluate(self, scenario, facts):
        return {'fixture': True}
''')
    spec = importlib.util.spec_from_file_location('cp3_concurrent_dependency_fixture', path)
    module = importlib.util.module_from_spec(spec)
    monkeypatch.setitem(sys.modules, spec.name, module)
    spec.loader.exec_module(module)
    dependency = _ServiceDependency(module.__name__, 'Oracle', 'evaluate')
    barrier = Barrier(4)

    def resolve():
        barrier.wait(timeout=5)
        return dependency.resolve()

    with ThreadPoolExecutor(max_workers=4) as pool:
        resolved = list(pool.map(lambda _: resolve(), range(4)))
    assert all(value is resolved[0] for value in resolved)
    assert module.Oracle.created == 1


# Preserve the three SPEC invalid expectations at the public binding boundary.
def test_binding_detects_live_oracle_dependency_behind_copied_module_slot(monkeypatch):
    import sys
    from types import ModuleType, SimpleNamespace
    import pytest
    registry = topology_registry()
    registry.bind_services(TOPOLOGY_TARGET)
    name = 'r2f.modules.s1.topology_oracles'
    original = sys.modules[name]
    copied = ModuleType(name)
    copied.__dict__.update(original.__dict__)
    monkeypatch.setitem(sys.modules, name, copied)
    binding = registry.bind_services(TOPOLOGY_TARGET)
    binding.verify_unchanged()
    monkeypatch.setattr(original, 'evaluate_topology', lambda *args: {'probe': 'oracle'})
    assert binding.services.oracle.evaluate(SimpleNamespace(), {}) == {'probe': 'oracle'}
    with pytest.raises(ValueError, match='service_binding'):
        binding.verify_unchanged()
    with pytest.raises(ValueError, match='service_binding'):
        registry.bind_services(TOPOLOGY_TARGET)


def test_binding_detects_live_fact_dependency_behind_copied_module_slot(monkeypatch):
    import sys
    from types import ModuleType
    import pytest
    registry = topology_registry()
    registry.bind_services(TOPOLOGY_TARGET)
    name = 'r2f.campaign.topology_adapter'
    original = sys.modules[name]
    copied = ModuleType(name)
    copied.__dict__.update(original.__dict__)
    monkeypatch.setitem(sys.modules, name, copied)
    binding = registry.bind_services(TOPOLOGY_TARGET)
    binding.verify_unchanged()
    monkeypatch.setattr(original, 'facts_from_events', lambda *args: {'probe': 'fact'})
    assert binding.services.fact_producer.produce(
        {'events': [], 'runtime': {}, 'config': {}}) == {'probe': 'fact'}
    with pytest.raises(ValueError, match='service_binding'):
        binding.verify_unchanged()
    with pytest.raises(ValueError, match='service_binding'):
        registry.bind_services(TOPOLOGY_TARGET)


def test_binding_rejects_equal_code_entry_with_unmeasured_resolver_globals(monkeypatch):
    from types import FunctionType
    import pytest
    from r2f.modules.s1 import topology_oracles
    registry = topology_registry()
    registry.bind_services(TOPOLOGY_TARGET)
    original = topology_oracles.TopologyOracle.evaluate
    namespace = dict(original.__globals__)
    namespace['resolve_evaluator'] = lambda evaluator_id: lambda scenario, facts: {'fixture': True}
    replacement = FunctionType(original.__code__, namespace, original.__name__,
                               original.__defaults__, original.__closure__)
    replacement.__module__ = original.__module__
    replacement.__qualname__ = original.__qualname__
    monkeypatch.setattr(topology_oracles.TopologyOracle, 'evaluate', replacement)
    with pytest.raises(ValueError, match='service_binding'):
        registry.bind_services(TOPOLOGY_TARGET)


def test_binding_survives_harmless_constant_references():
    from r2f.modules.s1 import topology_oracles
    registry = topology_registry()
    binding = registry.bind_services(TOPOLOGY_TARGET)
    binding.verify_unchanged()
    held_constants = topology_oracles.evaluate_topology.__code__.co_consts
    try:
        binding.verify_unchanged()
        assert registry.bind_services(TOPOLOGY_TARGET).descriptor_sha256 == binding.descriptor_sha256
    finally:
        del held_constants
    binding.verify_unchanged()


def test_twelve_concurrent_public_bindings_keep_unchanged_descriptor():
    from concurrent.futures import ThreadPoolExecutor
    from threading import Barrier
    from r2f.modules.s1 import topology_oracles
    registry = topology_registry()
    legacy = registry.get_services(TOPOLOGY_TARGET)
    baseline = registry.bind_services(TOPOLOGY_TARGET)
    barrier = Barrier(12)

    def query(_):
        held_constants = topology_oracles.evaluate_topology.__code__.co_consts
        barrier.wait(timeout=10)
        binding = registry.bind_services(TOPOLOGY_TARGET)
        binding.verify_unchanged()
        assert binding.descriptor_sha256 == baseline.descriptor_sha256
        # Keep introspection references alive through the concurrent queries.
        assert held_constants is topology_oracles.evaluate_topology.__code__.co_consts
        return binding

    with ThreadPoolExecutor(max_workers=12) as pool:
        bindings = list(pool.map(query, range(12)))
    assert registry.get_services(TOPOLOGY_TARGET) is legacy
    for binding in bindings:
        binding.verify_unchanged()
        assert binding.services.oracle is baseline.services.oracle
        assert binding.services.fact_producer is baseline.services.fact_producer
    baseline.verify_unchanged()
