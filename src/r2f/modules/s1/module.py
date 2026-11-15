"""One registry module; execution and reporting remain framework services."""
from copy import deepcopy
from .targets import DISPOSITIONS, EXECUTION_ORDER, target_declarations
from .providers import service_bindings


class S1Module:
    id = "s1"
    execution_order = EXECUTION_ORDER

    def __init__(self):
        self.targets = target_declarations()
        self.dispositions = deepcopy(DISPOSITIONS)
        self.services = service_bindings(self.targets)

    def to_dict(self):
        return {"id": self.id, "targets": {key: value.to_dict() for key, value in self.targets.items()},
                "execution_order": list(self.execution_order), "dispositions": deepcopy(self.dispositions)}


def register(registry):
    module = S1Module()
    registry.register(module)
    return module


def resolve_services(target_id):
    """Resolve the same registered services for ingestion and read-only audits."""
    from r2f.core import Registry
    registry = Registry()
    register(registry)
    return registry.get_services(target_id)
