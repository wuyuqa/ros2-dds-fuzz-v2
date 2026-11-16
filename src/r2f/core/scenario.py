"""Compilation is deterministic planning and never claims execution."""

from dataclasses import dataclass, field

from ._json import integer, nonempty, snapshot
from .evidence import INPUT_SCHEMA, INPUT_SCHEMA_V2, canonical_hash, effective_input_hash
from .semantic_target import SemanticTarget


@dataclass
class Scenario:
    target: SemanticTarget
    rmw: str
    arm: str
    repeat: int
    seed: int = 0
    parameters: dict = field(default_factory=dict)
    # Optional versioned exploration plan.  None must serialize exactly like
    # the historical scenario: no extra key, no changed canonical hash.
    mutation_plan: object = None

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
        if self.mutation_plan is not None:
            from .mutation_plan import MutationPlan
            if isinstance(self.mutation_plan, dict):
                self.mutation_plan = MutationPlan.from_dict(self.mutation_plan)
            if not isinstance(self.mutation_plan, MutationPlan):
                raise ValueError("mutation_plan must be a MutationPlan or None")
            self.mutation_plan.validate()
            # Own a private copy: a caller mutating its plan must not silently
            # change this scenario's identity.
            self.mutation_plan = MutationPlan.from_dict(self.mutation_plan.to_dict())

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

    def effective_input_v2(self, final_input=None, input_layer_receipts=None, source_manifest=None):
        """Versioned identity that also binds the exploration plan.

        The final submitted config must equal the plan's ``final_config`` when a
        plan is attached.  This prevents a plan and the executed input from
        describing two different inputs while sharing one identity.
        """
        if self.mutation_plan is not None:
            plan_final = snapshot(self.mutation_plan.final_config)
            # Without an explicit submission the plan's final config *is* the
            # submitted config.  Scenario.parameters may still carry the
            # mutation slot/carrier selectors and are not "input" here.
            submitted = plan_final if final_input is None else snapshot(final_input)
            if canonical_hash(plan_final) != canonical_hash(submitted):
                raise ValueError("mutation_plan.final_config differs from final submitted input")
        else:
            submitted = snapshot(self.parameters if final_input is None else final_input)
        effective = self.effective_input(final_input=submitted,
                                         input_layer_receipts=input_layer_receipts,
                                         source_manifest=source_manifest)
        effective["input_identity_schema"] = INPUT_SCHEMA_V2
        if self.mutation_plan is not None:
            plan = self.mutation_plan
            effective["mutation_plan"] = plan.to_dict()
            effective["operations"] = [op.to_dict() for op in plan.operations]
            effective["runtime_windows"] = snapshot(plan.runtime_windows)
            effective["contract_hash"] = plan.contract_hash
        contract = self.target.exploration_contract
        if contract is not None:
            effective["contract"] = contract.to_dict()
        return effective

    def input_hash_v2(self, **kwargs):
        return effective_input_hash(self.effective_input_v2(**kwargs), schema=INPUT_SCHEMA_V2)

    def to_dict(self):
        data = {"target": self.target.to_dict(), "target_id": self.target_id,
                "target_revision": self.target_revision, "rmw": self.rmw, "arm": self.arm,
                "repeat": self.repeat, "seed": self.seed, "parameters": snapshot(self.parameters),
                "mutation_kind": self.mutation_kind, "carrier": self.carrier,
                "mutation_parameters": self.mutation_parameters, "execution_stage": "planned",
                "state_steps": [s.to_dict() for s in self.state_steps],
                "input_identity_schema": INPUT_SCHEMA}
        if self.mutation_plan is not None:
            data["mutation_plan"] = self.mutation_plan.to_dict()
        return data

    def input_hash(self, **kwargs):
        return effective_input_hash(self.effective_input(**kwargs))

    @classmethod
    def from_dict(cls, data):
        plan = data.get("mutation_plan")
        return cls(SemanticTarget.from_dict(data["target"]), data["rmw"], data["arm"],
                   data["repeat"], data.get("seed", 0), data.get("parameters", {}), plan)


def compile_scenario(target, rmw, arm, repeat, seed=0, parameters=None, mutation_plan=None) -> Scenario:
    if isinstance(target, dict):
        target = SemanticTarget.from_dict(target)
    if not isinstance(target, SemanticTarget):
        raise ValueError("target must be a SemanticTarget")
    return Scenario(target, rmw, arm, repeat, seed, {} if parameters is None else parameters,
                    mutation_plan)
