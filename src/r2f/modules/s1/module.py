"""One registry module; execution and reporting remain framework services."""
from copy import deepcopy
from .targets import DISPOSITIONS, EXECUTION_ORDER
from .catalog import catalog_declarations
from .providers import exploratory_service_bindings, service_bindings


class S1Module:
    """Required compatibility view of the unified S1 catalog.

    ``targets``, ``execution_order`` and ``dispositions`` are the frozen
    required catalog.  Exploratory targets are a separate, explicitly
    requested registration and never appear here, so the required catalog hash
    cannot drift when fuzz targets are added.
    """
    id = "s1"
    execution_order = EXECUTION_ORDER

    def __init__(self):
        self.targets = catalog_declarations()
        self.dispositions = deepcopy(DISPOSITIONS)
        self.services = service_bindings(self.targets)

    def to_dict(self):
        return {"id": self.id, "targets": {key: value.to_dict() for key, value in self.targets.items()},
                "execution_order": list(self.execution_order), "dispositions": deepcopy(self.dispositions)}


class S1ExploratoryModule:
    """Exploratory catalog view over the same registry and runner.

    This is not a second module system: it carries the legacy fuzz targets and
    their services, plus explicitly opted-in capabilities, so the required module
    stays untouched.  Its ``execution_order`` is only catalog order; required
    sequencing is intentionally absent because
    exploratory runs are driven by corpus/feedback selection, not a fixed
    matrix.
    """
    id = "s1.exploratory"

    def __init__(self, *, runtime_capability=None):
        self.targets = catalog_declarations(exploratory=True,
                                            runtime_capability=runtime_capability)
        self.dispositions = {}
        self.services = exploratory_service_bindings(self.targets)

    @property
    def execution_order(self):
        return tuple(self.targets)

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


def register_exploratory(registry, *, include_required=False, runtime_capability=None):
    """Register the exploratory targets on an existing registry.

    ``include_required`` registers the required module first when the caller
    wants a combined registry; the exploratory module then adds its legacy
    targets plus an explicitly requested runtime capability. Duplicate ids
    still fail atomically in the Registry.
    """
    modules = []
    if include_required:
        modules.append(register(registry))
    module = S1ExploratoryModule(runtime_capability=runtime_capability)
    registry.register(module)
    modules.append(module)
    return modules


def resolve_services(target_id, *, exploratory=False, runtime_capability=None):
    """Resolve services for ingestion and read-only audits.

    Both registration views consume the unified catalog. The explicit scope
    and capability still select the registry contents: family membership never
    aliases a required identity to a fuzz contract (or vice versa).
    """
    from r2f.core import Registry
    registry = Registry()
    if exploratory:
        register_exploratory(registry, include_required=False,
                             runtime_capability=runtime_capability)
    else:
        register(registry)
    return registry.get_services(target_id)
