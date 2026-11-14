"""Compilation is deterministic planning and never claims execution."""

from dataclasses import dataclass, field

from ._json import integer, nonempty, snapshot
from .evidence import INPUT_SCHEMA, canonical_hash, effective_input_hash
from .semantic_target import SemanticTarget


@dataclass
class Scenario:
    target: SemanticTarget
    rmw: str
    arm: str
    repeat: int
    seed: int = 0
    parameters: dict = field(default_factory=dict)

    def __post_init__(self):
        self.target = SemanticTarget.from_dict(self.target.to_dict())
        nonempty(self.rmw, "rmw")
        if self.arm not in {"baseline", "transparent", "mutated", "negative"}:
            raise ValueError("unsupported scenario arm")
        integer(self.repeat, "repeat")
        integer(self.seed, "seed")
        if self.target.rmw_applicability and self.rmw not in self.target.rmw_applicability:
            raise ValueError("RMW is not declared applicable to target")
        self.parameters = snapshot(self.parameters)
        if not isinstance(self.parameters, dict):
            raise ValueError("parameters must be a dict")
        kind = self.mutation_kind
        carrier = self.carrier
        if not any(slot.mutation_kind == kind and slot.carrier == carrier for slot in self.target.mutation_slots):
            raise ValueError("scenario must use a declared mutation slot/carrier")

    @property
    def target_id(self):
        return self.target.target_id

    @property
    def target_revision(self):
        return self.target.target_revision

    @property
    def state_steps(self):
        return self.target.state_steps

    @property
    def execution_stage(self):
        from .execution import ExecutionStage
        return ExecutionStage.PLANNED

    @property
    def mutation_kind(self):
        default = self.target.mutation_slots[0].mutation_kind if self.target.mutation_slots else ""
        return self.parameters.get("mutation_kind", self.target.expected_effect_set.get("mutation_kind", default))

    @property
    def carrier(self):
        default = self.target.mutation_slots[0].carrier if self.target.mutation_slots else ""
        return self.parameters.get("carrier", self.target.expected_effect_set.get("carrier", default))

    @property
    def mutation_parameters(self):
        default = next((slot.parameters for slot in self.target.mutation_slots
                        if slot.mutation_kind == self.mutation_kind and slot.carrier == self.carrier), {})
        return snapshot(self.parameters.get("mutation_parameters", default))

    def effective_input(self, final_input=None, input_layer_receipts=None, source_manifest=None):
        """final_input is the actual submitted config/payload, not an execution result."""
        submitted = self.parameters if final_input is None else final_input
        return {"target_id": self.target_id, "target_revision": self.target_revision,
                "target_sha256": canonical_hash(self.target.to_dict()), "mutation_kind": self.mutation_kind,
                "carrier": self.carrier, "mutation_parameters": self.mutation_parameters,
                "rmw": self.rmw, "repeat": self.repeat, "seed": self.seed, "arm": self.arm,
                "mutation_layer_order": snapshot(self.parameters.get("mutation_layer_order", [self.mutation_kind])),
                "input_layer_receipts": snapshot(input_layer_receipts or []), "final_input": snapshot(submitted),
                "state_steps": [step.to_dict() for step in self.state_steps],
                "source_manifest": snapshot(source_manifest or {})}

    def to_dict(self):
        return {"target": self.target.to_dict(), "target_id": self.target_id,
                "target_revision": self.target_revision, "rmw": self.rmw, "arm": self.arm,
                "repeat": self.repeat, "seed": self.seed, "parameters": snapshot(self.parameters),
                "mutation_kind": self.mutation_kind, "carrier": self.carrier,
                "mutation_parameters": self.mutation_parameters, "execution_stage": "planned",
                "state_steps": [s.to_dict() for s in self.state_steps],
                "input_identity_schema": INPUT_SCHEMA}

    def input_hash(self, **kwargs):
        return effective_input_hash(self.effective_input(**kwargs))

    @classmethod
    def from_dict(cls, data):
        return cls(SemanticTarget.from_dict(data["target"]), data["rmw"], data["arm"],
                   data["repeat"], data.get("seed", 0), data.get("parameters", {}))


def compile_scenario(target, rmw, arm, repeat, seed=0, parameters=None) -> Scenario:
    if isinstance(target, dict):
        target = SemanticTarget.from_dict(target)
    if not isinstance(target, SemanticTarget):
        raise ValueError("target must be a SemanticTarget")
    return Scenario(target, rmw, arm, repeat, seed, {} if parameters is None else parameters)
