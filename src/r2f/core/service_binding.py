"""Measured service metadata, separate from execution and qualification.

A binding pins Python instances and entry points in this process. Its detached
JSON descriptor identifies source bytes, never object addresses. Callers verify
before and after calling the returned services; verification emits no receipt.
This is not a transitive dependency or frozen-source qualification mechanism.
"""
import dis
import inspect
import sys
from hashlib import sha256
from pathlib import Path
from struct import pack
from types import CodeType

from ._json import snapshot
from .evidence import canonical_hash
from .interfaces import ExecutionServices


_METHODS = {
    'mutation_provider': ('prepare',),
    'carrier': ('accepts', 'apply'),
    'fact_producer': ('produce',),
    'oracle': ('evaluate',),
}


def _code_value(value):
    """Encode compiled code by value, without reference-count or alias markers.

    Nested code and immutable constants are code content, not discovered callable
    dependencies. Defaults, closures and runtime globals are outside this digest.
    """
    if isinstance(value, CodeType):
        fields = (
            'co_argcount', 'co_posonlyargcount', 'co_kwonlyargcount',
            'co_nlocals', 'co_stacksize', 'co_flags', 'co_code', 'co_consts',
            'co_names', 'co_varnames', 'co_filename', 'co_name', 'co_qualname',
            'co_firstlineno', 'co_linetable', 'co_exceptiontable',
            'co_freevars', 'co_cellvars',
        )
        return ['code', {name: _code_value(getattr(value, name))
                         for name in fields if hasattr(value, name)}]
    if value is None:
        return ['none']
    if value is Ellipsis:
        return ['ellipsis']
    if isinstance(value, bool):
        return ['bool', value]
    if isinstance(value, int):
        return ['int', hex(value)]
    if isinstance(value, float):
        return ['float', pack('!d', value).hex()]
    if isinstance(value, complex):
        return ['complex', pack('!d', value.real).hex(), pack('!d', value.imag).hex()]
    if isinstance(value, str):
        return ['str', value]
    if isinstance(value, bytes):
        return ['bytes', value.hex()]
    if isinstance(value, tuple):
        return ['tuple', [_code_value(item) for item in value]]
    if isinstance(value, frozenset):
        return ['frozenset', sorted((_code_value(item) for item in value),
                                    key=canonical_hash)]
    raise ValueError('service_binding_uninspectable_code_constant')


def _source(component):
    try:
        source = inspect.getsourcefile(component)
        if not source:
            raise ValueError('source unavailable')
        path = Path(source).resolve(strict=True)
        digest = sha256(path.read_bytes()).hexdigest()
        module, qualname = component.__module__, component.__qualname__
    except (AttributeError, TypeError, OSError, ValueError) as exc:
        raise ValueError('service_binding_uninspectable_component') from exc
    result = {'module': module, 'qualname': qualname,
              'source_file': str(path), 'source_sha256': digest}
    if inspect.isfunction(component):
        result['code_sha256'] = canonical_hash(_code_value(component.__code__))
    return result


def _measure(service, methods):
    descriptor = {'class': _source(type(service)), 'callables': {}, 'dependency': None}
    identities = [service, type(service)]
    for name in methods:
        method = getattr(service, name, None)
        if not inspect.ismethod(method) or method.__self__ is not service \
                or not inspect.isfunction(method.__func__):
            raise ValueError('service_binding_ambiguous_callable:' + name)
        descriptor['callables'][name] = _source(method.__func__)
        identities.extend((method.__self__, method.__func__, method.__func__.__code__))
    return descriptor, identities


def _namespace(function, identities):
    """Pin the actual globals separately from the advertised module slot."""
    module = sys.modules.get(function.__module__)
    if module is None or function.__globals__.get('__name__') != function.__module__:
        raise ValueError('service_binding_changed:dependency_namespace')
    identities.extend((module, function.__globals__))
    return function.__module__


def _bind_dependency_namespaces(service, methods, dependencies, measured, identities):
    """Check only declared slots against their direct loaded global lookups.

    No recursive helper discovery, state/default/closure inspection or execution.
    Copied namespaces are allowed only when declared lookups match live slots.
    """
    if not dependencies:
        return
    callers = [(name, getattr(service, name).__func__) for name in methods]
    callers.extend((item['module'] + '.' + item['symbol'], function)
                   for item, function in dependencies)
    namespaces = [(name, function, _namespace(function, identities))
                  for name, function in callers]
    for (declaration, function), metadata in zip(dependencies, measured):
        metadata['namespace_module'] = _namespace(function, identities)
        lookups = []
        for name, caller, namespace in namespaces:
            if namespace == declaration['module'] and any(
                    instruction.opname == 'LOAD_GLOBAL'
                    and instruction.argval == declaration['symbol']
                    for instruction in dis.get_instructions(caller)):
                if caller.__globals__.get(declaration['symbol']) is not function:
                    raise ValueError('service_binding_changed:dependency_lookup')
                lookups.append(name)
        if not lookups:
            raise ValueError('service_binding_changed:dependency_lookup_unavailable')
        metadata['lookup_callables'] = lookups


def _resolve(service, methods):
    declaration = getattr(service, 'binding_dependency', None)
    if declaration is None:
        if hasattr(service, 'dependency') or callable(getattr(service, 'resolve', None)):
            raise ValueError('service_binding_no_resolver_declaration')
        descriptor, identities = _measure(service, methods)
        return service, descriptor, identities
    keys = {'module', 'symbol', 'method', 'callable_dependencies'}
    if not isinstance(declaration, dict) or set(declaration) != keys \
            or any(not isinstance(declaration[key], str) or not declaration[key]
                   for key in ('module', 'symbol', 'method')) \
            or methods != (declaration['method'],):
        raise ValueError('service_binding_ambiguous_dependency')
    resolver = getattr(service, 'resolve', None)
    if not inspect.ismethod(resolver) or resolver.__self__ is not service:
        raise ValueError('service_binding_no_resolver')
    resolved = resolver()
    if resolved is service or getattr(resolved, 'binding_dependency', None) is not None:
        raise ValueError('service_binding_unresolved_component')
    module = sys.modules.get(declaration['module'])
    implementation = getattr(module, declaration['symbol'], None)
    if not inspect.isclass(implementation) or type(resolved) is not implementation:
        raise ValueError('service_binding_dependency_changed')
    descriptor, identities = _measure(resolved, methods)
    dependencies = declaration['callable_dependencies']
    if not isinstance(dependencies, list):
        raise ValueError('service_binding_ambiguous_dependency')
    measured, seen, functions = [], set(), []
    for dependency in dependencies:
        if not isinstance(dependency, dict) or set(dependency) != {'module', 'symbol'} \
                or any(not isinstance(value, str) or not value for value in dependency.values()):
            raise ValueError('service_binding_ambiguous_dependency')
        slot = (dependency['module'], dependency['symbol'])
        if slot in seen:
            raise ValueError('service_binding_ambiguous_dependency')
        seen.add(slot)
        dependency_module = sys.modules.get(slot[0])
        function = getattr(dependency_module, slot[1], None)
        if not inspect.isfunction(function):
            raise ValueError('service_binding_uninspectable_dependency')
        measured.append({'module': slot[0], 'symbol': slot[1], 'implementation': _source(function)})
        identities.extend((dependency_module, function, function.__code__))
        functions.append((dependency, function))
    _bind_dependency_namespaces(resolved, methods, functions, measured, identities)
    descriptor['dependency'] = {'module': declaration['module'], 'symbol': declaration['symbol'],
                                'method': declaration['method'], 'callable_dependencies': measured,
                                'resolver': _source(resolver.__func__),
                                'resolver_class': _source(type(service))}
    identities.extend((service, type(service), resolver.__func__,
                       resolver.__func__.__code__, module, implementation))
    return resolved, descriptor, identities


class ServiceBinding:
    """Fixed ExecutionServices plus detached measurements; no authority or use."""

    def __init__(self, registry, target_id):
        self._registry = registry
        self._target_id = target_id
        self._services, self._descriptor, self._identities = self._capture()

    def _capture(self):
        registry, target_id = self._registry, self._target_id
        target = registry.get_target(target_id)
        services = registry.get_services(target_id)
        components, actual, identities = {}, {}, [services]
        for role, methods in _METHODS.items():
            resolved, component, refs = _resolve(getattr(services, role), methods)
            actual[role] = resolved
            components[role] = component
            identities.extend(refs)
        return ExecutionServices(**actual), {
            'schema': 'r2f.execution-service-binding/v1',
            'owner_module_id': registry._target_modules[target_id],
            'target_id': target_id, 'target_revision': target.target_revision,
            'target_manifest_sha256': canonical_hash(target.to_dict()),
            'components': components,
        }, tuple(identities)

    @property
    def services(self):
        return self._services

    @property
    def descriptor(self):
        return snapshot(self._descriptor)

    @property
    def descriptor_sha256(self):
        return canonical_hash(self._descriptor)

    def verify_unchanged(self):
        """Raise on current scope, source, instance or callable drift."""
        _, descriptor, identities = self._capture()
        if descriptor != self._descriptor or len(identities) != len(self._identities) \
                or any(now is not before for now, before in zip(identities, self._identities)):
            raise ValueError('service_binding_changed')
