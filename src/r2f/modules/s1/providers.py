"""S1 services over the existing runtime and raw-evidence adapters.

Preparation describes inputs only. Execution dependencies are passed explicitly
and never become part of a scenario, capsule, catalog or effective input hash.
"""
from copy import deepcopy

from r2f.core import ExecutionServices, Scenario, canonical_hash, effective_input_hash
from r2f.core.evidence import INPUT_SCHEMA, INPUT_SCHEMA_V2
from r2f.core.mutation_plan import (
    ExplorationContract, MutationPlan, MutationProposal, validate_plan,
)
from .targets import (EXECUTION_ORDER, EXPLORATORY_RUNTIME_TARGET,
                      EXPLORATORY_TARGET_ORDER)


_KINDS = dict(zip(EXECUTION_ORDER, ('late_joiner', 'late_joiner',
                                  'discovery_drop', 'endpoint_qos_variation')))


def _accepts(scenario, targets, carrier=None):
    if not isinstance(scenario, Scenario):
        return False
    target = targets.get(scenario.target_id)
    if target is None:
        return False
    try:
        # Revalidate mutable scenario fields against the registered declaration.
        Scenario.from_dict(scenario.to_dict())
        if canonical_hash(scenario.target.to_dict()) != canonical_hash(target.to_dict()):
            return False
        required_kind = _KINDS.get(scenario.target_id)
        if required_kind is None:
            # Exploratory targets are not in the required fixed-kind map; their
            # accepted (mutation_kind, carrier) pairs come from the declaration.
            # An undeclared pair is refusal, never a fallback to the required
            # path or to another slot of the same target.
            allowed = {(slot.mutation_kind, slot.carrier)
                       for slot in target.mutation_slots}
            if (scenario.mutation_kind, scenario.carrier) not in allowed:
                return False
        elif scenario.mutation_kind != required_kind:
            return False
        return carrier is None or scenario.carrier == carrier
    except (ValueError, KeyError, TypeError, AttributeError):
        return False


class S1MutationProvider:
    def __init__(self, targets):
        self._targets = deepcopy(targets)

    def prepare(self, scenario):
        """Return the existing deterministic layer description, without I/O."""
        if not _accepts(scenario, self._targets):
            raise ValueError('unsupported S1 mutation scenario')
        parameters = scenario.mutation_parameters
        layer = {'layer': scenario.carrier, 'declared_mutation': scenario.mutation_kind}
        if scenario.target_id == EXECUTION_ORDER[2]:
            layer.update(implementation='isolated_netns_nft_discovery_udp_drop', parameters=parameters)
        elif scenario.target_id == EXECUTION_ORDER[3]:
            layer['parameters'] = parameters
        else:
            if 'reader_delay_sec' not in parameters:
                raise ValueError('prepared late_joiner requires submitted reader_delay_sec')
            layer['submitted_reader_delay_sec'] = parameters['reader_delay_sec']
        return {'mutation_kind': scenario.mutation_kind, 'carrier': scenario.carrier,
                'mutation_parameters': parameters, 'input_layer_receipts': [layer]}


class S1Carrier:
    def __init__(self, targets, carrier):
        self._targets = deepcopy(targets)
        self._carrier = carrier

    def accepts(self, scenario):
        return _accepts(scenario, self._targets, self._carrier)

    def apply(self, scenario, prepared, *, execution):
        """Invoke the existing runtime once and return its complete envelope.

        The input identity schema is dispatched explicitly.  Legacy scenarios
        keep the historical v1 contract byte-for-byte; a plan-bearing scenario
        is executed only with ``r2f.input-identity/v2``.  An unknown schema, a
        v1 digest for a plan, or a v2 digest under a non-v2 config is refused
        instead of being downgraded.
        """
        if not self.accepts(scenario):
            raise ValueError('unsupported S1 carrier scenario')
        if not isinstance(prepared, dict):
            raise ValueError('carrier prepared input must be a dict')
        config = execution.get('config') if isinstance(execution, dict) else None
        config_schema = config.get('input_identity_schema') if isinstance(config, dict) else None
        schema = prepared.get('input_identity_schema')
        if scenario.mutation_plan is not None:
            if schema != INPUT_SCHEMA_V2:
                raise ValueError('plan-bearing carrier input requires r2f.input-identity/v2')
            return self._apply_v2(scenario, prepared, execution=execution)
        if schema not in (None, INPUT_SCHEMA):
            raise ValueError('unsupported carrier input identity schema: ' + repr(schema))
        if config_schema not in (None, INPUT_SCHEMA):
            raise ValueError('v1 prepared input cannot use a non-v1 execution identity')
        return self._apply_v1(scenario, prepared, execution=execution)

    def _apply_v1(self, scenario, prepared, *, execution):
        """Historical v1 path; behaviour and canonical identity are unchanged."""
        expected = scenario.effective_input(final_input=prepared['final_input'],
                                            input_layer_receipts=prepared['input_layer_receipts'],
                                            source_manifest=prepared['source_manifest'])
        if canonical_hash(prepared) != canonical_hash(expected):
            raise ValueError('carrier prepared input differs from scenario')
        digest = effective_input_hash(prepared)
        config = deepcopy(execution['config'])
        if canonical_hash(config.get('effective_input_sha256')) != canonical_hash(digest):
            raise ValueError('carrier effective input hash mismatch')
        # Canonical JSON distinguishes bool/int and int/float, including
        # nested values. Python equality would admit False as repeat=0.
        bindings = {'final_input': prepared['final_input'],
                    'input_layer_receipts': prepared['input_layer_receipts'],
                    'source_manifest': prepared['source_manifest'],
                    'scenario': scenario.to_dict()}
        if any(canonical_hash(config.get(key)) != canonical_hash(value)
               for key, value in bindings.items()):
            raise ValueError('carrier config/capsule input binding mismatch')
        for key in ('target_id', 'target_revision', 'rmw', 'arm', 'repeat', 'seed',
                    'mutation_kind', 'carrier'):
            if canonical_hash(config.get(key)) != canonical_hash(expected[key]):
                raise ValueError('carrier execution identity mismatch:' + key)
        for key, value in prepared['final_input'].items():
            if canonical_hash(config.get(key)) != canonical_hash(value):
                raise ValueError('carrier final input mismatch:' + key)
        runtime = execution['runtime']
        if not callable(runtime):
            raise ValueError('carrier requires explicit runtime callable')
        return runtime(config, execution['output_dir'])

    def _apply_v2(self, scenario, prepared, *, execution):
        """Plan-bearing v2 path: one identity for plan, capsule and runtime."""
        if prepared.get('input_identity_schema') != INPUT_SCHEMA_V2:
            raise ValueError('carrier requires r2f.input-identity/v2')
        plan = scenario.mutation_plan
        if plan is None:
            raise ValueError('v2 carrier input requires a scenario mutation_plan')
        missing = [key for key in ('mutation_plan', 'operations', 'runtime_windows',
                                   'contract_hash', 'final_input', 'input_layer_receipts',
                                   'source_manifest')
                   if key not in prepared]
        if missing:
            raise ValueError('v2 effective input is missing: ' + ','.join(missing))
        if canonical_hash(prepared['mutation_plan']) != canonical_hash(plan.to_dict()):
            raise ValueError('carrier prepared plan differs from scenario')
        if canonical_hash(prepared['operations']) != canonical_hash(
                [operation.to_dict() for operation in plan.operations]):
            raise ValueError('carrier prepared operations differ from plan')
        if canonical_hash(prepared['runtime_windows']) != canonical_hash(plan.runtime_windows):
            raise ValueError('carrier prepared runtime windows differ from plan')
        if prepared['contract_hash'] != plan.contract_hash:
            raise ValueError('carrier prepared contract hash differs from plan')
        if canonical_hash(plan.final_config) != canonical_hash(prepared['final_input']):
            raise ValueError('carrier final input differs from plan.final_config')
        expected = scenario.effective_input_v2(
            final_input=prepared['final_input'],
            input_layer_receipts=prepared['input_layer_receipts'],
            source_manifest=prepared['source_manifest'])
        if canonical_hash(prepared) != canonical_hash(expected):
            raise ValueError('carrier prepared input differs from scenario v2 identity')
        digest = effective_input_hash(prepared, schema=INPUT_SCHEMA_V2)
        config = deepcopy(execution['config'])
        if config.get('input_identity_schema') != INPUT_SCHEMA_V2:
            raise ValueError('v2 carrier requires a v2 runtime config identity')
        if canonical_hash(config.get('effective_input_sha256')) != canonical_hash(digest):
            raise ValueError('carrier effective input hash mismatch (v2)')
        bindings = {'final_input': prepared['final_input'],
                    'input_layer_receipts': prepared['input_layer_receipts'],
                    'source_manifest': prepared['source_manifest'],
                    'scenario': scenario.to_dict()}
        if any(canonical_hash(config.get(key)) != canonical_hash(value)
               for key, value in bindings.items()):
            raise ValueError('carrier config/capsule input binding mismatch (v2)')
        config_plan = config.get('mutation_plan')
        if not isinstance(config_plan, dict):
            raise ValueError('v2 runtime config is missing mutation_plan')
        if canonical_hash(config_plan) != canonical_hash(plan.to_dict()):
            raise ValueError('carrier runtime plan differs from scenario')
        if canonical_hash(config_plan.get('operations')) != canonical_hash(prepared['operations']):
            raise ValueError('carrier runtime plan operations differ from payload')
        if canonical_hash(config_plan.get('runtime_windows')) != canonical_hash(
                prepared['runtime_windows']):
            raise ValueError('carrier runtime plan windows differ from payload')
        if config_plan.get('contract_hash') != prepared['contract_hash']:
            raise ValueError('carrier runtime plan contract hash differs from payload')
        if ('mutation_plan_sha256' in config
                and config['mutation_plan_sha256'] != canonical_hash(plan.to_dict())):
            raise ValueError('carrier runtime mutation plan hash mismatch')
        if 'effective_input' in config and canonical_hash(
                config['effective_input']) != canonical_hash(prepared):
            raise ValueError('carrier runtime effective input differs from capsule')
        for key in ('target_id', 'target_revision', 'rmw', 'arm', 'repeat', 'seed',
                    'mutation_kind', 'carrier'):
            if canonical_hash(config.get(key)) != canonical_hash(expected[key]):
                raise ValueError('carrier execution identity mismatch:' + key)
        for key, value in prepared['final_input'].items():
            if canonical_hash(config.get(key)) != canonical_hash(value):
                raise ValueError('carrier final input mismatch:' + key)
        runtime = execution['runtime']
        if not callable(runtime):
            raise ValueError('carrier requires explicit runtime callable')
        return runtime(config, execution['output_dir'])


class S1FactProducer:
    def produce(self, raw):
        # Import only at invocation: projection/recovery adapters already import
        # runtime_adapter helpers, and module registration must stay offline.
        from r2f.campaign.runtime_adapter import facts_from_events
        return facts_from_events(raw['events'], raw['runtime'], raw['config'])


class S1Oracle:
    def evaluate(self, scenario, facts):
        from .oracles import evaluate_primary
        return evaluate_primary(facts, scenario.target_id)


def service_bindings(targets):
    mutation = S1MutationProvider(targets)
    facts, oracle = S1FactProducer(), S1Oracle()
    carriers = {name: S1Carrier(targets, name) for name in ('ros2_endpoint', 'wire_proxy')}
    return {target_id: ExecutionServices(mutation, carriers['wire_proxy' if target_id == EXECUTION_ORDER[2]
                                                           else 'ros2_endpoint'], facts, oracle)
            for target_id in targets}


# ---------------------------------------------------------------------------
# Exploratory provider: consumes a deterministic MutationPlan.
#
# ``prepare`` is still input description only.  The difference from the
# required provider is that the final input is *materialised from the plan*
# rather than read off a fixed per-target parameter map, and that the plan is
# re-validated against declared runtime capabilities on every call.  Nothing
# here opens a socket, starts a process, or records a hit.
# ---------------------------------------------------------------------------

#: Catalog activation classes the runtime can currently route.  A surface
#: outside this set is refusal, never a silent fallback to the required path.
EXPLORATORY_ACTIVATION_CAPABILITIES = {
    "activation": ("F1",),
    "mutators": ("M01", "M02", "M03"),
    "runtime_bindings": (
        "late_joiner.reader_delay_sec",
        "projection.qos_profile",
        "discovery.drop_duration_sec",
        "pair.history_and_sample_counts",
        "pair.publish_interval_sec",
    ),
    "multi_window": False,
}


class ExploratoryInputError(ValueError):
    """Raised with the exact surface/operation that cannot be prepared live."""


def _exploratory_base_input(target_id):
    """The reviewed semantic default payload for the reused runtime backend."""
    from r2f.campaign.identity import semantic_input

    runtime_target = EXPLORATORY_RUNTIME_TARGET.get(target_id)
    if runtime_target is None:
        raise ExploratoryInputError(
            f"{target_id}: no live runtime backend is wired for this target")
    return semantic_input("mutated", None, target_id=runtime_target)


def _plan_capabilities(contract):
    return dict(EXPLORATORY_ACTIVATION_CAPABILITIES)


def validate_exploratory_plan(plan, contract):
    """Validate one plan against the exploratory runtime capability set."""
    result = validate_plan(plan, capabilities=_plan_capabilities(contract),
                           contract=contract)
    if result.status != "valid":
        raise ExploratoryInputError(
            f"plan is not live-routable ({result.status}): " + "; ".join(result.reasons))
    return result


def _operation_inputs(plan):
    """Per-operation before/after payloads, reconstructed from the chain.

    The plan stores hashes, not payloads, so the payload is replayed from the
    parent plus each operation's declared changes only when the caller kept
    them.  Here the plan is authoritative and the payload is the plan's
    ``final_config``; intermediate payloads are not fabricated.
    """
    return deepcopy(plan.final_config)


class S1ExploratoryMutationProvider:
    """Deterministic plan consumer; no RNG is evaluated inside ``prepare``."""

    def __init__(self, targets):
        self._targets = deepcopy(targets)
        self._contracts = {
            target_id: target.exploration_contract
            for target_id, target in self._targets.items()
            if target.exploration_contract is not None
        }

    def contract_for(self, target_id):
        contract = self._contracts.get(target_id)
        if contract is None:
            raise ExploratoryInputError(f"{target_id}: no exploration contract declared")
        return contract

    def prepare_plan(self, scenario, plan):
        """Materialise the final input described by ``plan``.

        The same ``plan`` and ``rng_seed`` always produce the same result;
        no mutator is re-run here.
        """
        if not isinstance(plan, MutationPlan):
            plan = MutationPlan.from_dict(plan)
        contract = self.contract_for(scenario.target_id)
        if not contract.surfaces:
            pending = scenario.target.observation_contract.get("pending_surface_ids", [])
            raise ExploratoryInputError(
                f"{scenario.target_id}: no live routable surface "
                f"(capability pending: {','.join(pending) or 'unspecified'}); "
                "refusing to prepare a live input")
        validate_exploratory_plan(plan, contract)
        if plan.rng_seed != scenario.seed:
            raise ExploratoryInputError(
                "plan rng_seed does not match the scenario seed; refusing to "
                "silently regenerate a different input")
        submitted = _operation_inputs(plan)
        expected_after = (plan.operations[-1].input_after_sha256 if plan.operations
                          else plan.parent_content_sha256)
        if canonical_hash(submitted) != expected_after:
            raise ExploratoryInputError(
                "plan final_config does not match the last operation input_after hash")

        statuses = {}
        for operation in plan.operations:
            status = operation.applicability_status
            statuses.setdefault(status, []).append(operation.op_id)
        if "rejected" in statuses or "blocked" in statuses:
            raise ExploratoryInputError(
                "plan contains non-applicable operations: " + ",".join(
                    statuses["rejected"] + statuses.get("blocked", [])))

        receipts = [{
            "layer": scenario.carrier,
            "declared_mutation": scenario.mutation_kind,
            "plan_hash": plan.plan_hash(),
            "content_sha256": plan.content_hash(),
            "rng_algorithm": plan.rng_algorithm,
            "rng_seed": plan.rng_seed,
            "operation_status": {op_id: status for status, op_ids in statuses.items()
                                  for op_id in op_ids},
            "runtime_windows": deepcopy(plan.runtime_windows),
        }]
        return {
            "input_identity_schema": INPUT_SCHEMA_V2,
            "plan_hash": plan.plan_hash(),
            "plan_content_sha256": plan.content_hash(),
            "mutation_kind": scenario.mutation_kind,
            "carrier": scenario.carrier,
            "mutation_parameters": deepcopy(scenario.mutation_parameters),
            "final_input": submitted,
            "input_layer_receipts": receipts,
            "plan_status": "valid",
        }

    def prepare(self, scenario):
        """Prepare from the scenario's own embedded plan, if it carries one.

        The required provider keeps its fixed payload path; this method exists
        so a plan-bearing exploratory scenario can share the same carrier and
        runner plumbing without a second execution path.
        """
        plan = scenario.parameters.get("mutation_plan")
        if plan is None:
            raise ExploratoryInputError(
                f"{scenario.target_id}: exploratory prepare requires an explicit mutation_plan; "
                "refusing to fall back to a fixed required payload")
        return self.prepare_plan(scenario, plan)


class S1ExploratoryCarrier(S1Carrier):
    """Carrier over the reused runtime backend; identity stays the new target.

    The exploratory target id is checked here, but the identity checks are the
    shared schema-dispatching carrier path: a plan-bearing input is validated
    and executed as v2 only.  There is no v1 digest under a v2 report.
    """

    def apply(self, scenario, prepared, *, execution):
        if scenario.target_id not in EXPLORATORY_TARGET_ORDER:
            raise ExploratoryInputError("not an exploratory target: " + scenario.target_id)
        if not isinstance(prepared, dict):
            raise ExploratoryInputError("prepared exploratory input must be a dict")
        if scenario.mutation_plan is None:
            raise ExploratoryInputError(
                "exploratory execution requires a plan-bearing v2 input; "
                "refusing a plan-less exploratory payload")
        if prepared.get("input_identity_schema") != INPUT_SCHEMA_V2:
            raise ExploratoryInputError(
                "exploratory carrier requires r2f.input-identity/v2; refusing v1 identity")
        config = execution.get("config") if isinstance(execution, dict) else None
        if not isinstance(config, dict) or config.get("input_identity_schema") != INPUT_SCHEMA_V2:
            raise ExploratoryInputError(
                "exploratory carrier requires a v2 runtime config identity")
        try:
            return super().apply(scenario, prepared, execution=execution)
        except ValueError as exc:
            raise ExploratoryInputError(str(exc))


class S1ExploratoryOracle(S1Oracle):
    """Reuse the reviewed evaluator; compatibility uses its own guard."""

    def evaluate(self, scenario, facts):
        if scenario.target_id == "discovery.match_compatibility.fuzz_v1":
            from .oracles import evaluate_match_compatibility_guard
            return evaluate_match_compatibility_guard(facts)
        runtime_target = EXPLORATORY_RUNTIME_TARGET.get(scenario.target_id)
        if runtime_target is None:
            raise ExploratoryInputError("no oracle backend for " + scenario.target_id)
        from .oracles import evaluate_primary
        return evaluate_primary(facts, runtime_target)


def exploratory_service_bindings(targets):
    provider = S1ExploratoryMutationProvider(targets)
    facts = S1FactProducer()
    oracle = S1ExploratoryOracle()
    carriers = {
        "ros2_endpoint": S1ExploratoryCarrier(targets, "ros2_endpoint"),
        "wire_proxy": S1ExploratoryCarrier(targets, "wire_proxy"),
    }
    bindings = {}
    for target_id, target in targets.items():
        carrier_id = (target.mutation_slots[0].carrier
                      if target.mutation_slots else "ros2_endpoint")
        if carrier_id not in carriers:
            raise ExploratoryInputError(
                f"{target_id}: unsupported exploratory carrier {carrier_id}")
        bindings[target_id] = ExecutionServices(provider, carriers[carrier_id], facts, oracle)
    return bindings
