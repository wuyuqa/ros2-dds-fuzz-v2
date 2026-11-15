"""Small structural interfaces; implementations need no inheritance."""

from dataclasses import dataclass
from typing import Any, Mapping, Protocol

from .semantic_target import SemanticTarget
from .scenario import Scenario


class TargetProvider(Protocol):
    targets: Mapping[str, SemanticTarget]


class Module(TargetProvider, Protocol):
    id: str


class MutationProvider(Protocol):
    def prepare(self, scenario: Scenario) -> dict: ...


class Carrier(Protocol):
    def accepts(self, scenario: Scenario) -> bool: ...
    def apply(self, scenario: Scenario, prepared: dict, *,
              execution: Mapping[str, Any]) -> dict: ...


class FactProducer(Protocol):
    def produce(self, raw: Mapping[str, Any]) -> dict: ...


class Oracle(Protocol):
    def evaluate(self, scenario: Scenario, facts: Mapping[str, Any]) -> dict: ...


@dataclass(frozen=True)
class ExecutionServices:
    """Runtime service references; never serialized into a target or catalog."""

    mutation_provider: MutationProvider
    carrier: Carrier
    fact_producer: FactProducer
    oracle: Oracle
