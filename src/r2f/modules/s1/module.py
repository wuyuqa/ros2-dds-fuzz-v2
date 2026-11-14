"""One registry module; execution and reporting remain framework services."""
from copy import deepcopy
from .targets import DISPOSITIONS, EXECUTION_ORDER, target_declarations


class S1Module:
    id = "s1"
    execution_order = EXECUTION_ORDER

    def __init__(self):
        self.targets = target_declarations()
        self.dispositions = deepcopy(DISPOSITIONS)

    def to_dict(self):
        return {"id": self.id, "targets": {key: value.to_dict() for key, value in self.targets.items()},
                "execution_order": list(self.execution_order), "dispositions": deepcopy(self.dispositions)}


def register(registry):
    module = S1Module()
    registry.register(module)
    return module
