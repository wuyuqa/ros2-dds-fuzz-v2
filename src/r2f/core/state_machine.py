"""Declarative state steps and a callback-driven, receipt-producing executor."""

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Callable, Mapping

from ._json import nonempty, snapshot


def utc_now():
    return datetime.now(timezone.utc).isoformat()


@dataclass
class StateCondition:
    condition_id: str
    fact: str
    operator: str = "eq"
    expected: Any = True

    def __post_init__(self):
        nonempty(self.condition_id, "condition_id")
        nonempty(self.fact, "fact")
        if self.operator not in {"eq", "ne", "in", "exists", "gt", "ge", "lt", "le"}:
            raise ValueError("unsupported condition operator")
        self.expected = snapshot(self.expected)

    def evaluate(self, state: Mapping) -> bool:
        value = state
        for part in self.fact.split("."):
            if not isinstance(value, Mapping) or part not in value:
                return False
            value = value[part]
        if self.operator == "exists":
            return True
        # JSON booleans must not accidentally satisfy integer identity contracts.
        if self.operator in {"eq", "ne"}:
            equal = type(value) is type(self.expected) and value == self.expected
            return equal if self.operator == "eq" else not equal
        try:
            if self.operator == "in":
                return any(type(value) is type(item) and value == item for item in self.expected)
            if type(value) not in (int, float) or type(self.expected) not in (int, float):
                return False
            return {"gt": lambda: value > self.expected, "ge": lambda: value >= self.expected,
                    "lt": lambda: value < self.expected, "le": lambda: value <= self.expected}[self.operator]()
        except (TypeError, ValueError):
            return False

    def to_dict(self):
        return {"condition_id": self.condition_id, "fact": self.fact,
                "operator": self.operator, "expected": snapshot(self.expected)}

    @classmethod
    def from_dict(cls, data):
        return cls(**data)


@dataclass
class StateStep:
    step_id: str
    action: str
    requires: list[StateCondition] = field(default_factory=list)
    parameters: dict = field(default_factory=dict)

    def __post_init__(self):
        nonempty(self.step_id, "step_id")
        nonempty(self.action, "action")
        self.requires = [StateCondition.from_dict(c) if isinstance(c, dict) else c for c in self.requires]
        if any(not isinstance(c, StateCondition) for c in self.requires):
            raise ValueError("requires must contain StateCondition objects")
        self.parameters = snapshot(self.parameters)
        if not isinstance(self.parameters, dict):
            raise ValueError("step parameters must be a dict")
        if len({c.condition_id for c in self.requires}) != len(self.requires):
            raise ValueError("duplicate condition_id")

    def to_dict(self):
        return {"step_id": self.step_id, "action": self.action,
                "requires": [c.to_dict() for c in self.requires], "parameters": snapshot(self.parameters)}

    @classmethod
    def from_dict(cls, data):
        return cls(**data)


@dataclass
class StepReceipt:
    step_id: str
    action: str
    before: dict
    after: dict
    requires: dict
    status: str
    started_at: str
    finished_at: str
    result: Any = None
    error: str = ""

    def to_dict(self):
        return snapshot(vars(self))


class StateExecutor:
    def plan(self, steps):
        return [step.to_dict() for step in steps]

    def execute(self, steps, state: dict, perform: Callable):
        """perform(action, parameters, state) may update state and returns JSON evidence."""
        receipts = []
        for step in steps:
            before = snapshot(state)
            checks = {c.condition_id: c.evaluate(before) for c in step.requires}
            start = utc_now()
            result, error = None, ""
            if not all(checks.values()):
                status = "blocked"
                error = "requires not satisfied"
            else:
                try:
                    result = snapshot(perform(step.action, snapshot(step.parameters), state))
                    status = "executed"
                except Exception as exc:
                    status, error = "failed", f"{type(exc).__name__}: {exc}"
            receipts.append(StepReceipt(step.step_id, step.action, before, snapshot(state),
                                        checks, status, start, utc_now(), result, error))
            if status != "executed":
                break
        return receipts
