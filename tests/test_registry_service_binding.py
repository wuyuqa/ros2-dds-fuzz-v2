"""Offline public service binding contract; no execution qualification."""
import hashlib
import inspect
from pathlib import Path

from r2f.core import Registry, ExecutionServices, canonical_hash
from r2f.modules.s1.module import register


def test_registered_real_services_have_detached_deterministic_binding():
    registry = Registry()
    register(registry)
    target_id = next(iter(registry.targets))
    legacy = registry.get_services(target_id)
    binding = registry.bind_services(target_id)
    assert isinstance(binding.services, ExecutionServices)
    assert binding.services.oracle is legacy.oracle
    assert binding.services.fact_producer is legacy.fact_producer
    descriptor = binding.descriptor
    assert descriptor['target_id'] == target_id
    assert descriptor['target_revision'] == registry.get_target(target_id).target_revision
    assert descriptor['owner_module_id'] == 's1'
    assert descriptor['target_manifest_sha256'] == canonical_hash(registry.get_target(target_id).to_dict())
    oracle = descriptor['components']['oracle']
    assert oracle['class']['qualname'] == type(legacy.oracle).__qualname__
    method = oracle['callables']['evaluate']
    assert method['qualname'] == legacy.oracle.evaluate.__func__.__qualname__
    assert method['source_sha256'] == hashlib.sha256(Path(inspect.getsourcefile(legacy.oracle.evaluate)).read_bytes()).hexdigest()
    assert binding.descriptor_sha256 == canonical_hash(descriptor)
    assert binding.verify_unchanged() is None
    descriptor['components']['oracle']['class']['qualname'] = 'caller changed'
    assert binding.descriptor['components']['oracle']['class']['qualname'] != 'caller changed'
    assert registry.bind_services(target_id).descriptor_sha256 == binding.descriptor_sha256
    assert registry.get_services(target_id) is legacy
    assert set(binding.descriptor) == {'schema', 'owner_module_id', 'target_id', 'target_revision',
                                       'target_manifest_sha256', 'components'}


def test_loaded_callable_code_is_measured_separately_from_unchanged_file():
    """Same source bytes/name cannot conceal a changed in-memory entry point."""
    import types
    registry = Registry()
    register(registry)
    target_id = next(iter(registry.targets))
    service = registry.get_services(target_id).oracle
    before = registry.bind_services(target_id)
    original = service.evaluate
    # Keep the original module/qualname/file while changing actual Python code.
    changed = types.FunctionType((lambda self, scenario, facts: {'replacement': True}).__code__,
                                 original.__func__.__globals__, original.__func__.__name__)
    changed.__module__ = original.__func__.__module__
    changed.__qualname__ = original.__func__.__qualname__
    changed.__code__ = changed.__code__.replace(co_filename=original.__func__.__code__.co_filename)
    service.evaluate = types.MethodType(changed, service)
    after = registry.bind_services(target_id)
    assert after.descriptor_sha256 != before.descriptor_sha256
    assert after.descriptor['components']['oracle']['callables']['evaluate']['code_sha256']


def test_descriptor_is_deterministic_across_processes_and_invocation():
    import os
    import subprocess
    import sys
    script = '''from r2f.core import Registry
from r2f.modules.s1.module import register
r = Registry(); register(r)
b = r.bind_services(next(iter(r.targets)))
print(b.descriptor_sha256)
'''
    results = [subprocess.check_output([sys.executable, '-c', script],
               env=dict(os.environ, PYTHONPATH='src', PYTHONDONTWRITEBYTECODE='1'), text=True).strip()
               for _ in range(2)]
    assert results[0] == results[1]


def test_callable_replacement_with_identical_code_fails_process_identity_check():
    import types
    import pytest
    registry = Registry()
    register(registry)
    target_id = next(iter(registry.targets))
    binding = registry.bind_services(target_id)
    service = binding.services.oracle
    original = service.evaluate.__func__
    changed = types.FunctionType(original.__code__, original.__globals__, original.__name__)
    changed.__module__, changed.__qualname__ = original.__module__, original.__qualname__
    service.evaluate = types.MethodType(changed, service)
    assert registry.bind_services(target_id).descriptor_sha256 == binding.descriptor_sha256
    with pytest.raises(ValueError, match='service_binding_changed'):
        binding.verify_unchanged()


def test_public_target_snapshot_drift_is_rejected_but_detached_edit_is_not():
    import pytest
    registry = Registry()
    register(registry)
    target_id = next(iter(registry.targets))
    binding = registry.bind_services(target_id)
    registry.get_target(target_id).target_revision = 1
    binding.verify_unchanged()
    registry.targets[target_id].target_revision = 1
    with pytest.raises(ValueError, match='service_binding_changed'):
        binding.verify_unchanged()


def test_unknown_target_and_target_without_services_fail_closed():
    import pytest
    from types import SimpleNamespace
    from r2f.core import SemanticTarget
    registry = Registry()
    with pytest.raises(KeyError):
        registry.bind_services('unknown')
    registry.register(SimpleNamespace(id='no-services', targets={'test': SemanticTarget('test', 'fixture')}))
    with pytest.raises(ValueError, match='no execution services'):
        registry.bind_services('test')
    with pytest.raises(ValueError, match='no execution services'):
        registry.get_services('test')


def fixture_registry(tmp_path, monkeypatch):
    """An explicitly registered Python fixture, never a DDS implementation."""
    import importlib.util
    import sys
    from dataclasses import replace
    from r2f.modules.s1.module import S1Module
    path = tmp_path / 'cp3_bound_service_fixture.py'
    path.write_text('''class Oracle:
    def evaluate(self, scenario, facts):
        if facts.get('during_call'):
            facts['during_call']()
        return {'fixture': True, 'value': facts['value']}
class Facts:
    def produce(self, raw):
        return {'fixture': True, 'value': raw['value']}
''')
    spec = importlib.util.spec_from_file_location('cp3_bound_service_fixture', path)
    module = importlib.util.module_from_spec(spec)
    monkeypatch.setitem(sys.modules, spec.name, module)
    spec.loader.exec_module(module)
    owner = S1Module()
    target_id = next(iter(owner.targets))
    owner.services[target_id] = replace(owner.services[target_id], oracle=module.Oracle(), fact_producer=module.Facts())
    registry = Registry()
    registry.register(owner)
    return registry, target_id, module, path


def test_real_fixture_calls_verify_before_after_and_detect_file_drift(tmp_path, monkeypatch):
    import pytest
    registry, target_id, module, path = fixture_registry(tmp_path, monkeypatch)
    binding = registry.bind_services(target_id)
    original = path.read_bytes()
    binding.verify_unchanged()
    facts = binding.services.fact_producer.produce({'value': 7})
    assert facts == {'fixture': True, 'value': 7}
    assert binding.services.oracle.evaluate(None, facts) == {'fixture': True, 'value': 7}
    binding.verify_unchanged()
    path.write_bytes(original + b'\n# drift before invocation\n')
    with pytest.raises(ValueError, match='service_binding_changed'):
        binding.verify_unchanged()
    path.write_bytes(original)
    binding.verify_unchanged()
    result = binding.services.oracle.evaluate(None, {
        'value': 8, 'during_call': lambda: path.write_bytes(original + b'\n# drift during invocation\n')})
    assert result == {'fixture': True, 'value': 8}
    with pytest.raises(ValueError, match='service_binding_changed'):
        binding.verify_unchanged()


def test_service_instance_replacement_fails_even_when_durable_descriptor_matches(tmp_path, monkeypatch):
    import pytest
    registry, target_id, module, path = fixture_registry(tmp_path, monkeypatch)
    binding = registry.bind_services(target_id)
    legacy = registry.get_services(target_id)
    object.__setattr__(legacy, 'oracle', module.Oracle())
    assert registry.bind_services(target_id).descriptor_sha256 == binding.descriptor_sha256
    with pytest.raises(ValueError, match='service_binding_changed'):
        binding.verify_unchanged()


def test_missing_resolver_and_uninspectable_component_are_not_bound(tmp_path, monkeypatch):
    import pytest
    registry, target_id, module, path = fixture_registry(tmp_path, monkeypatch)
    legacy = registry.get_services(target_id)
    module.Oracle.binding_dependency = {
        'module': module.__name__, 'symbol': 'Oracle', 'method': 'evaluate', 'callable_dependencies': []}
    with pytest.raises(ValueError, match='no_resolver'):
        registry.bind_services(target_id)
    assert registry.get_services(target_id) is legacy
    del module.Oracle.binding_dependency
    # A structural callable accepted by legacy registration cannot prove source identity.
    legacy.oracle.evaluate = print
    with pytest.raises(ValueError, match='ambiguous_callable'):
        registry.bind_services(target_id)
    assert registry.get_services(target_id) is legacy
    del legacy.oracle.evaluate
    path.unlink()
    with pytest.raises(ValueError, match='uninspectable_component'):
        registry.bind_services(target_id)


def test_binding_code_digest_detects_constant_change_with_source_unchanged(tmp_path, monkeypatch):
    import pytest
    registry, target_id, module, path = fixture_registry(tmp_path, monkeypatch)
    original_source = path.read_bytes()
    original = module.Oracle.evaluate.__code__
    binding = registry.bind_services(target_id)
    old_digest = binding.descriptor['components']['oracle']['callables']['evaluate']['code_sha256']
    # Change executable code constants in memory, keeping the source file intact.
    changed = tuple('changed_value' if item == 'value' else item for item in original.co_consts)
    monkeypatch.setattr(module.Oracle.evaluate, '__code__', original.replace(co_consts=changed))
    assert binding.services.oracle.evaluate(None, {'changed_value': 9}) == {'fixture': True, 'value': 9}
    assert path.read_bytes() == original_source
    with pytest.raises(ValueError, match='service_binding_changed'):
        binding.verify_unchanged()
    fresh = registry.bind_services(target_id)
    assert fresh.descriptor['components']['oracle']['callables']['evaluate']['code_sha256'] != old_digest
    fresh.verify_unchanged()


def test_binding_pins_code_identity_even_for_equal_code_content(tmp_path, monkeypatch):
    import pytest
    registry, target_id, module, path = fixture_registry(tmp_path, monkeypatch)
    binding = registry.bind_services(target_id)
    original = module.Oracle.evaluate.__code__
    equal_copy = original.replace()
    assert equal_copy is not original
    monkeypatch.setattr(module.Oracle.evaluate, '__code__', equal_copy)
    assert registry.bind_services(target_id).descriptor_sha256 == binding.descriptor_sha256
    with pytest.raises(ValueError, match='service_binding_changed'):
        binding.verify_unchanged()
