"""Dedicated topology services; validation never regenerates a mutation plan."""
from copy import deepcopy
from importlib import import_module
from threading import Lock

from r2f.core import ExecutionServices, canonical_hash
from r2f.core.evidence import INPUT_SCHEMA_V2
from r2f.core.mutation_plan import MutationPlan, validate_plan
from .topology_targets import (TOPOLOGY_TARGET, TOPOLOGY_CAPABILITY, TOPOLOGY_CARRIER,
                               TOPOLOGY_MUTATION_KIND, TOPOLOGY_BINDING)


class TopologyMutationProvider:
    def __init__(self, targets):
        from .topology_targets import topology_target_declaration
        if set(targets) != {TOPOLOGY_TARGET} or canonical_hash(targets[TOPOLOGY_TARGET].to_dict()) != \
                canonical_hash(topology_target_declaration().to_dict()):
            raise ValueError("topology_provider_requires_exact_opted_in_target")
        self._targets = deepcopy(targets)

    def prepare_plan(self, scenario, plan):
        from .providers import _accepts, ExploratoryInputError
        from .topology_input import (topology_contract, topology_input_layer_receipts,
                                     validate_topology_plan)
        if not _accepts(scenario, self._targets, TOPOLOGY_CARRIER) or scenario.arm != "mutated":
            raise ExploratoryInputError("topology_requires_registered_mutated_endpoint_program")
        if scenario.target.observation_contract.get("runtime_capability") != TOPOLOGY_CAPABILITY:
            raise ExploratoryInputError("topology_runtime_capability_required")
        plan = MutationPlan.from_dict(plan.to_dict() if isinstance(plan, MutationPlan) else plan)
        contract = topology_contract()
        validate_topology_plan(plan)
        if scenario.mutation_plan is None or canonical_hash(scenario.mutation_plan.to_dict()) != \
                canonical_hash(plan.to_dict()):
            raise ExploratoryInputError("topology_scenario_plan_mismatch")
        if type(scenario.seed) is not int or plan.rng_seed != scenario.seed:
            raise ExploratoryInputError("topology_plan_rng_seed_mismatch")
        validation = validate_plan(plan, contract=contract, capabilities={
            "activation": ("F2",), "mutators": tuple(contract.surfaces[0].mutator_ids),
            "surface_kinds": ("action_program",), "runtime_bindings": (TOPOLOGY_BINDING,),
            "multi_window": False,
        })
        if validation.status not in {"valid", "needs_observation"} or any(
                op.applicability_status in {"rejected", "blocked"} for op in plan.operations):
            raise ExploratoryInputError("topology_plan_not_applicable:" + validation.status + ":" +
                                        ";".join(validation.reasons))
        final = deepcopy(plan.final_config)
        if set(final) != {"topology_program"}:
            raise ExploratoryInputError("topology_final_input_keys_mismatch")
        return {"input_identity_schema": INPUT_SCHEMA_V2, "plan_hash": plan.plan_hash(),
                "plan_content_sha256": plan.content_hash(), "mutation_kind": TOPOLOGY_MUTATION_KIND,
                "carrier": TOPOLOGY_CARRIER, "mutation_parameters": deepcopy(scenario.mutation_parameters),
                "final_input": final, "input_layer_receipts": topology_input_layer_receipts(plan),
                "plan_status": validation.status, "execution_admitted": False}

    def prepare(self, scenario):
        if scenario.mutation_plan is None:
            raise ValueError("topology_prepare_requires_embedded_core_plan")
        return self.prepare_plan(scenario, scenario.mutation_plan)


class _ServiceDependency:
    """Resolve the independent owner implementation lazily, never synthesize pass."""
    def __init__(self, module, symbol, method, *, callable_dependencies=()):
        self.module, self.symbol, self.method = module, symbol, method
        self.dependency = module + "." + symbol
        self._resolved = None
        self._resolve_lock = Lock()
        self.callable_dependencies = tuple(callable_dependencies)

    @property
    def binding_dependency(self):
        """Explicit import slot for metadata binding, never execution evidence."""
        return {"module": self.module, "symbol": self.symbol, "method": self.method,
                "callable_dependencies": [{"module": module, "symbol": symbol}
                                          for module, symbol in self.callable_dependencies]}

    def resolve(self):
        """Instantiate once for explicit binding; legacy calls remain lazy."""
        with self._resolve_lock:
            if self._resolved is None:
                try:
                    implementation = getattr(import_module(self.module), self.symbol)
                except (ImportError, AttributeError) as exc:
                    raise ValueError("topology_service_dependency_unavailable:" + self.dependency) from exc
                self._resolved = implementation()
            return self._resolved

    def _invoke(self, *args):
        try:
            implementation = getattr(import_module(self.module), self.symbol)
        except (ImportError, AttributeError) as exc:
            raise ValueError("topology_service_dependency_unavailable:" + self.dependency) from exc
        return getattr(implementation(), self.method)(*args)

    def produce(self, raw):
        return self._invoke(raw)

    def evaluate(self, scenario, facts):
        return self._invoke(scenario, facts)


def topology_service_bindings(targets):
    from .providers import S1Carrier

    class TopologyCarrier(S1Carrier):
        def apply(self, scenario, prepared, *, execution):
            config = execution.get("config", {}) if isinstance(execution, dict) else {}
            if config.get("runtime_capability") != TOPOLOGY_CAPABILITY:
                raise ValueError("topology_runtime_capability_required")
            # Core only audits generic action-program shape. Re-audit the S1
            # program here even when callers bypass provider.prepare_plan.
            descriptor = TopologyMutationProvider(targets).prepare(scenario)
            if not isinstance(prepared, dict) or canonical_hash(prepared.get("input_layer_receipts")) != \
                    canonical_hash(descriptor["input_layer_receipts"]):
                raise ValueError("topology_input_layer_receipts_mismatch")
            return super().apply(scenario, prepared, execution=execution)

    return {TOPOLOGY_TARGET: ExecutionServices(
        TopologyMutationProvider(targets), TopologyCarrier(targets, TOPOLOGY_CARRIER),
        _ServiceDependency("r2f.campaign.topology_adapter", "TopologyFactProducer", "produce",
                           callable_dependencies=(("r2f.campaign.topology_adapter", "facts_from_events"),)),
        _ServiceDependency("r2f.modules.s1.topology_oracles", "TopologyOracle", "evaluate",
                           callable_dependencies=(("r2f.modules.s1.topology_oracles", "resolve_evaluator"),
                                                  ("r2f.modules.s1.topology_oracles", "evaluate_topology"))))}
