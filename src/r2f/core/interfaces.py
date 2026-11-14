"""Small structural interfaces; implementations need no inheritance."""

from typing import Any, Mapping, Protocol

from .semantic_target import SemanticTarget
from .scenario import Scenario
from .mutation import MutationReceipt


class TargetProvider(Protocol):
    targets: Mapping[str, SemanticTarget]


class Module(TargetProvider, Protocol):
    id: str


class MutationProvider(Protocol):
    def prepare(self, scenario: Scenario) -> dict: ...


class Carrier(Protocol):
    def accepts(self, scenario: Scenario) -> bool: ...
    def apply(self, scenario: Scenario, prepared: dict) -> MutationReceipt: ...


class FactProducer(Protocol):
    def produce(self, raw: Mapping[str, Any]) -> dict: ...


class Oracle(Protocol):
    def evaluate(self, scenario: Scenario, facts: Mapping[str, Any]) -> dict: ...
