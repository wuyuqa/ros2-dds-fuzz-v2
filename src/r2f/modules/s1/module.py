"""One registry module; execution and reporting remain framework services."""
from copy import deepcopy
from .targets import (DISPOSITIONS, EXECUTION_ORDER, EXPLORATORY_TARGET_ORDER,
                      exploratory_target_declarations, target_declarations)
from .providers import exploratory_service_bindings, service_bindings


class S1Module:
    """Required-scope module.

    ``targets``, ``execution_order`` and ``dispositions`` are the frozen
    required catalog.  Exploratory targets are a separate, explicitly
    requested registration and never appear here, so the required catalog hash
    cannot drift when fuzz targets are added.
    """
    id = "s1"
    execution_order = EXECUTION_ORDER

    def __init__(self):
        self.targets = target_declarations()
        self.dispositions = deepcopy(DISPOSITIONS)
        self.services = service_bindings(self.targets)

    def to_dict(self):
        return {"id": self.id, "targets": {key: value.to_dict() for key, value in self.targets.items()},
                "execution_order": list(self.execution_order), "dispositions": deepcopy(self.dispositions)}


class S1ExploratoryModule:
    """Explicit exploratory registration over the same registry and runner.

    This is not a second module system: it carries the five fuzz targets and
    their services so the required module stays untouched.  It has no
    ``execution_order``; required sequencing is intentionally absent because
    exploratory runs are driven by corpus/feedback selection, not a fixed
    matrix.
    """
    id = "s1.exploratory"

    def __init__(self):
        self.targets = exploratory_target_declarations()
        self.dispositions = {}
        self.services = exploratory_service_bindings(self.targets)

    @property
    def execution_order(self):
        return tuple(EXPLORATORY_TARGET_ORDER)

    def to_dict(self):
        return {"id": self.id,
                "targets": {key: value.to_dict() for key, value in self.targets.items()},
                "execution_order": list(self.execution_order),
                "campaign_kind": "exploratory",
                "required_catalog_included": False}


def register(registry):
    module = S1Module()
    registry.register(module)
    return module


def register_exploratory(registry, *, include_required=False):
    """Register the exploratory targets on an existing registry.

    ``include_required`` registers the required module first when the caller
    wants a combined registry; the exploratory module then adds only its own
    five targets.  Duplicate ids still fail atomically in the Registry.
    """
    modules = []
    if include_required:
        modules.append(register(registry))
    module = S1ExploratoryModule()
    registry.register(module)
    modules.append(module)
    return modules


def resolve_services(target_id, *, exploratory=False):
    """Resolve services for ingestion and read-only audits.

    The registry used depends on the declared scope; a required target is
    never resolved out of the exploratory catalog or vice versa.
    """
    from r2f.core import Registry
    registry = Registry()
    if exploratory:
        register_exploratory(registry, include_required=False)
    else:
        register(registry)
    return registry.get_services(target_id)
