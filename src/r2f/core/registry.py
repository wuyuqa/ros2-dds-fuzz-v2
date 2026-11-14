"""One module/target registry shared by the main runner."""

from collections.abc import Mapping
from types import MappingProxyType

from ._json import nonempty
from .semantic_target import SemanticTarget


class Registry:
    def __init__(self):
        self._modules = {}
        self._targets = {}

    @property
    def modules(self):
        return MappingProxyType(self._modules)

    @property
    def targets(self):
        return MappingProxyType(self._targets)

    def register(self, module):
        identifier = nonempty(getattr(module, "id", None), "module.id")
        if identifier in self._modules:
            raise ValueError(f"duplicate module: {identifier}")
        targets = getattr(module, "targets", None)
        if not isinstance(targets, Mapping) or not targets:
            raise ValueError("module.targets must be a nonempty mapping")
        prepared = {}
        for key, target in targets.items():
            if not isinstance(target, SemanticTarget) or key != target.target_id:
                raise ValueError("target mapping key must match SemanticTarget.target_id")
            if key in self._targets:
                raise ValueError(f"duplicate target: {key}")
            prepared[key] = SemanticTarget.from_dict(target.to_dict())
        self._modules[identifier] = module
        self._targets.update(prepared)
        return module

    def get_target(self, target_id):
        return SemanticTarget.from_dict(self._targets[target_id].to_dict())

    def get_module(self, module_id):
        return self._modules[module_id]
