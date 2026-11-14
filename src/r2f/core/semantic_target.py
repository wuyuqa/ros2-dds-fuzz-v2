"""Target manifests independent of any runtime or module implementation."""

from dataclasses import dataclass, field

from ._json import integer, nonempty, snapshot
from .state_machine import StateCondition, StateStep


@dataclass
class MutationSlot:
    mutation_kind: str
    carrier: str
    parameters: dict = field(default_factory=dict)

    def __post_init__(self):
        nonempty(self.mutation_kind, "mutation_kind")
        nonempty(self.carrier, "carrier")
        self.parameters = snapshot(self.parameters)
        if not isinstance(self.parameters, dict):
            raise ValueError("slot parameters must be a dict")

    def to_dict(self):
        return snapshot(vars(self))


@dataclass
class SemanticTarget:
    target_id: str
    intent_and_scope: str
    target_revision: int = 0
    state_preconditions: list[StateCondition] = field(default_factory=list)
    state_steps: list[StateStep] = field(default_factory=list)
    mutation_slots: list[MutationSlot] = field(default_factory=list)
    observation_contract: dict = field(default_factory=dict)
    expected_effect_set: dict = field(default_factory=dict)
    rule_contract: dict = field(default_factory=dict)
    evidence_policy: dict = field(default_factory=dict)
    rmw_applicability: list[str] = field(default_factory=list)
    control_contract: dict = field(default_factory=dict)

    def __post_init__(self):
        nonempty(self.target_id, "target_id")
        nonempty(self.intent_and_scope, "intent_and_scope")
        integer(self.target_revision, "target_revision")
        if self.target_revision not in (0, 1):
            raise ValueError("target_revision must be 0 or 1; use a new identity for later revisions")
        self.state_preconditions = [StateCondition.from_dict(c) if isinstance(c, dict) else c
                                    for c in self.state_preconditions]
        self.state_steps = [StateStep.from_dict(s) if isinstance(s, dict) else s for s in self.state_steps]
        self.mutation_slots = [MutationSlot(**s) if isinstance(s, dict) else s for s in self.mutation_slots]
        for items, cls, label in ((self.state_preconditions, StateCondition, "state_preconditions"),
                                  (self.state_steps, StateStep, "state_steps"),
                                  (self.mutation_slots, MutationSlot, "mutation_slots")):
            if any(not isinstance(item, cls) for item in items):
                raise ValueError(f"invalid {label}")
        if len({s.step_id for s in self.state_steps}) != len(self.state_steps):
            raise ValueError("duplicate step_id")
        if len({c.condition_id for c in self.state_preconditions}) != len(self.state_preconditions):
            raise ValueError("duplicate precondition identity")
        for name in ("observation_contract", "expected_effect_set", "rule_contract", "evidence_policy",
                     "control_contract"):
            value = snapshot(getattr(self, name))
            if not isinstance(value, dict):
                raise ValueError(f"{name} must be a dict")
            setattr(self, name, value)
        self.rmw_applicability = list(self.rmw_applicability)
        for rmw in self.rmw_applicability:
            nonempty(rmw, "rmw_applicability")

    def to_dict(self):
        return {"target_id": self.target_id, "intent_and_scope": self.intent_and_scope,
                "target_revision": self.target_revision,
                "state_preconditions": [c.to_dict() for c in self.state_preconditions],
                "state_steps": [s.to_dict() for s in self.state_steps],
                "mutation_slots": [s.to_dict() for s in self.mutation_slots],
                **{key: snapshot(getattr(self, key)) for key in (
                    "observation_contract", "expected_effect_set", "rule_contract", "evidence_policy",
                    "rmw_applicability", "control_contract")}}

    @classmethod
    def from_dict(cls, data):
        return cls(**snapshot(data))
