"""Opt-in topology INPUT bridge to the existing core plan; never execution.

v2 envelope content hashes deduplicate cases.  Raw program hashes, core config
hashes, plan hashes and effective-input identities have distinct contracts.
Runtime consumes ``plan.final_config`` unchanged; validation NEVER mutates.
"""
from __future__ import annotations

import json

from r2f.core._json import integer, snapshot
from r2f.core.evidence import canonical_hash, _deterministic_receipt
from r2f.core.mutation_plan import (
    EffectSpec, ExplorationContract, FactSpec, RuleSpec, MutationPlan, MutationProposal, RNG_ALGORITHM, compose,
    MutationSurface, MUTATION_CHAIN_MAX_LENGTH, ACTION_PROGRAM_LAYER_SCHEMA,
)
from r2f.core.mutators import TOPOLOGY_MUTATOR_ID, make_rng
from . import fuzz_mutators_v2 as v2, input_model, topology_model

TOPOLOGY_TARGET = "discovery.endpoint_topology.fuzz_v1"
TOPOLOGY_CAPABILITY = "topology_v1"
TOPOLOGY_MUTATION_KIND = "endpoint_program"
TOPOLOGY_CARRIER = "ros2_endpoint"
TOPOLOGY_SURFACE = "S1.topology.program"
TOPOLOGY_INPUT_SCHEMA = "r2f.topology-input/v1"
TOPOLOGY_LAYER_SCHEMA = ACTION_PROGRAM_LAYER_SCHEMA
MAX_PROGRAM_BYTES = 1024 * 1024

# Shared declarations for the separately implemented raw-backed resolver and
# evaluator.  These IDs are NOT evidence that either service ran or resolved.
TOPOLOGY_FACT_PRODUCER = "s1.topology.raw_fact_resolver.v1"
TOPOLOGY_FACT_EXECUTION = "s1.topology.execution_observed.v1"
TOPOLOGY_FACT_HEALTH = "s1.topology.tool_health_ok.v1"
TOPOLOGY_FACT_IDENTITY = "s1.topology.identity_bound.v1"
TOPOLOGY_FACT_GRAPH = "s1.topology.pair_graph_consistent.v1"
TOPOLOGY_FACT_DELIVERY = "s1.topology.delivery_consistent.v1"
TOPOLOGY_FACT_GUARDS = "s1.topology.guard_disposition_consistent.v1"
TOPOLOGY_FACT_IDS = (
    TOPOLOGY_FACT_EXECUTION, TOPOLOGY_FACT_HEALTH, TOPOLOGY_FACT_IDENTITY,
    TOPOLOGY_FACT_GRAPH, TOPOLOGY_FACT_DELIVERY, TOPOLOGY_FACT_GUARDS,
)
TOPOLOGY_RULE = "s1.topology.program_consistency.v1"
TOPOLOGY_EFFECT = "s1.topology.program_consistent.v1"
TOPOLOGY_EVALUATOR = "s1.topology.program_consistency.evaluate.v1"
TOPOLOGY_FORBIDDEN_RESIDUALS = (
    "topology.identity_mismatch", "topology.pair_graph_mismatch",
    "topology.closed_delivery_gap", "topology.guard_side_effect", "topology.cft_predicate_mismatch",
)


def topology_fact_specs():
    """Raw-backed aggregate booleans; missing/ambiguous evidence stays unknown.

    Identity/graph/delivery quantify over applicable directed pairs and current
    endpoint generations.  Input predictions and aggregate counts alone are
    not evidence.  Empty/inapplicable obligations need explicit resolver scope.
    """
    sources = {
        TOPOLOGY_FACT_EXECUTION: ["topology_provenance", "topology_source_checked", "topology_action_done"],
        TOPOLOGY_FACT_HEALTH: ["environment_preflight", "isolation_locks_acquired",
                               "topology_cleanup", "isolation_locks_released"],
        TOPOLOGY_FACT_IDENTITY: ["endpoint_created", "topology_endpoint_bound",
                                 "topology_generation_retired", "process_started"],
        TOPOLOGY_FACT_GRAPH: ["topology_pair_observation", "match_snapshot", "graph_snapshot", "command_done"],
        TOPOLOGY_FACT_DELIVERY: ["send", "receive", "command_done", "topology_action_done"],
        TOPOLOGY_FACT_GUARDS: ["topology_guard_reject", "state_step_after"],
    }
    return [FactSpec(
        id=fid, version=1, value_type="bool", unit="boolean", scope="run",
        sources=sources[fid], producer_id=TOPOLOGY_FACT_PRODUCER,
        freshness="current_action_window_raw_chronology",
        identity_binding="program/logical_endpoint/generation/GUID/PID/start_ticks/command",
        missing_policy="unknown") for fid in TOPOLOGY_FACT_IDS]


def topology_effect_spec():
    return EffectSpec(
        TOPOLOGY_EFFECT, 1, "topology_program_action_windows",
        ["observed_consistency", "expected_guard_rejection"],
        list(TOPOLOGY_FORBIDDEN_RESIDUALS))


def topology_rule_spec():
    return RuleSpec(
        id=TOPOLOGY_RULE, version=1,
        spec_refs=[topology_model.SCHEMA, "D009", "D013", "D019", "S1-C14/T10"],
        applies_when={"target_id": TOPOLOGY_TARGET, "runtime_capability": TOPOLOGY_CAPABILITY,
                      "requires": "raw-backed applicable pair/generation obligations"},
        required_fact_ids=list(TOPOLOGY_FACT_IDS), evaluator_id=TOPOLOGY_EVALUATOR,
        allowed_effects=[TOPOLOGY_EFFECT],
        forbidden_residuals=list(TOPOLOGY_FORBIDDEN_RESIDUALS),
        limitations=("Declaration only: resolver/evaluator implementation and execution are separate. "
                     "Predicted compatibility is not observed match; aggregate counts alone are insufficient. "
                     "Missing per-writer identity or raw chronology stays unknown. Finite trailing missing "
                     "delivery is not a violation; an interior gap requires a later same-generation sentinel. "
                     "Guard negatives, fixtures and tool failures are not SUT candidates. Forbidden residuals "
                     "take precedence over allowed observations. CFT predicate checks use only the independently "
                     "supported full String.data LIKE subset and authenticated sample observations; "
                     "configuration is not a match expectation. No live or maturity qualification implied."),
        responsibility_layer="s1", primary_or_supporting="primary")


def topology_contract():
    """Opt-in route/spec declarations, not live capability or oracle evidence."""
    return ExplorationContract(
        surfaces=[MutationSurface(
            surface_id=TOPOLOGY_SURFACE, version=1,
            source_refs=[topology_model.SCHEMA, v2.CASE_SCHEMA],
            surface_kind="action_program", input_paths=["topology_program"],
            value_type="action_program", unit="program",
            domain={"program_schema": topology_model.SCHEMA,
                    "input_extension": TOPOLOGY_INPUT_SCHEMA,
                    "max_program_bytes": MAX_PROGRAM_BYTES,
                    "delegate_manifest_sha256": canonical_hash({
                        "generator_version": v2.GENERATOR_VERSION,
                        "operators": v2.operator_manifest(),
                        "surface": v2.surface_by_id("S1.topology").to_dict()}),
                    "chain_max": MUTATION_CHAIN_MAX_LENGTH,
                    "validation": "r2f.modules.s1.topology_model.validate_program"},
            special_values=[], boundary_classes=[], mutator_ids=[TOPOLOGY_MUTATOR_ID],
            value_relations=[], tool_requires={"opt_in": TOPOLOGY_CAPABILITY},
            sut_guards={}, carrier_id=TOPOLOGY_CARRIER, runtime_phase="compile_time",
            runtime_binding="topology.program", allowed_with=[], conflicts_with=[],
            order_after=[], fact_ids=list(TOPOLOGY_FACT_IDS), rule_ids=[TOPOLOGY_RULE],
            effect_id=TOPOLOGY_EFFECT,
            missing_policy="reject", unsupported_policy="reject", activation="F2")],
        facts=topology_fact_specs(), rules=[topology_rule_spec()],
        effects=[topology_effect_spec()],
    )


def _program(program, *, expected=None, operator_id=None):
    program = snapshot(program)
    if len(json.dumps(program, ensure_ascii=False, allow_nan=False).encode()) > MAX_PROGRAM_BYTES:
        raise ValueError("topology program exceeds byte budget")
    validation = topology_model.validate_program(program)
    if validation["status"] != "valid":
        # A deliberately invalid guard document remains a named, REJECTED
        # input, never executable.  Unexpected/bulk over-budget inputs fail.
        errors = validation["errors"]
        intentional = expected == "expect_guard_reject" and bool(errors)
        for error in errors:
            path, code = error["path"], error["code"]
            allowed = (
                operator_id == "guard.structure" and code == "duplicate"
                or operator_id == "guard.parameter" and code == "budget"
                and path.endswith(".params.count")
                or operator_id == "filter.expr" and ".filter" in path and code == "value"
            )
            intentional = intentional and allowed
        if not intentional:
            raise ValueError("topology program invalid or over budget: " + str(errors))
        # The parameter guard is only the v2 single max+1 negative, not an
        # escape for arbitrary sample budgets.  No execution can admit it.
        if operator_id == "guard.parameter":
            maximum = program.get("budget", {}).get(
                "max_batch_size", topology_model.DEFAULT_BUDGET["max_batch_size"])
            if any(a.get("params", {}).get("count", 0) > maximum + 1
                   for a in program["actions"]):
                raise ValueError("guard.parameter exceeds the named max+1 negative")
    return program, validation


def _request(value, seed):
    value = snapshot(value)
    if not isinstance(value, dict) or not {"operator_id"} <= set(value) \
            or set(value) - {"operator_id", "slot_id", "context"}:
        raise ValueError("mutation_request requires operator_id and optional slot_id/context only")
    operator_id = value["operator_id"]
    surface = v2.surface_by_id("S1.topology")
    if not isinstance(operator_id, str) or operator_id not in v2.OPERATOR_SPECS:
        raise ValueError(f"unknown topology operator: {operator_id}")
    slots = [slot for slot in surface.slots if operator_id in slot.operators]
    if not slots:
        raise ValueError(f"operator not declared for topology: {operator_id}")
    slot_id = value.get("slot_id")
    if "slot_id" in value and (not isinstance(slot_id, str) or
                              slot_id not in {slot.slot_id for slot in slots}):
        raise ValueError(f"unknown/undeclared topology slot: {slot_id}")
    context = value.get("context", {})
    if not isinstance(context, dict):
        raise ValueError("mutation_request.context must be a dict")
    if "_operator_id" in context:
        raise ValueError("context cannot override the delegated operator")
    for mapping in (context, context.get("parameters", {})):
        if not isinstance(mapping, dict):
            raise ValueError("context.parameters must be a dict")
        for key in ("seed", "rng_seed"):
            if key in mapping and (type(mapping[key]) is not int or mapping[key] != seed):
                raise ValueError("topology layer seed mismatch")
        if "rng_algorithm" in mapping and mapping["rng_algorithm"] != RNG_ALGORITHM:
            raise ValueError("topology layer RNG algorithm mismatch")
    _deterministic_receipt(context)
    return {"operator_id": operator_id, **({"slot_id": slot_id} if slot_id else {}),
            "context": context}


def _status(expected):
    return {"status": "rejected" if expected == "expect_guard_reject" else
            "needs_observation" if expected in {"unknown_not_pass", "expect_guard_reject_or_unknown"}
            else "applicable", "expected_disposition": expected,
            "execution_evidence": False, "candidate_eligible": False}


def mutate_topology(input, surface, rng, context):
    """S1-specific delegate returning core MutationProposal; never a runtime step."""
    context = snapshot(context)
    if not isinstance(context, dict) or set(context) != {"rng_seed", "layer_index", "mutation_request"}:
        raise ValueError("topology mutator context requires rng_seed/layer_index/mutation_request")
    base_seed = integer(context["rng_seed"], "rng_seed")
    index = integer(context["layer_index"], "layer_index")
    if index >= MUTATION_CHAIN_MAX_LENGTH:
        raise ValueError("topology layer_index exceeds chain budget")
    seed = base_seed + index
    if not callable(getattr(rng, "getstate", None)) or rng.getstate() != make_rng(seed).getstate():
        raise ValueError("topology RNG seed/state mismatch")
    if not isinstance(surface, MutationSurface) or surface.to_dict() != topology_contract().surfaces[0].to_dict():
        raise ValueError("topology surface contract mismatch")
    if not isinstance(input, dict) or set(input) != {"topology_program"}:
        raise ValueError("topology input must contain topology_program only")
    before, _ = _program(input["topology_program"])
    request = _request(context["mutation_request"], seed)
    case = v2.mutate("S1.topology", request["operator_id"], before, seed=seed,
                     slot_id=request.get("slot_id"), context=request["context"])
    after, _ = _program(case.input, expected=case.expected_disposition,
                        operator_id=case.operator_id)
    if canonical_hash(before) == canonical_hash(after):
        raise ValueError("topology mutation has no content change")
    receipt = {
        "schema": TOPOLOGY_LAYER_SCHEMA, "layer_index": index,
        "input_path": "topology_program", "program_schema": topology_model.SCHEMA,
        "seed": seed, "operator_id": case.operator_id, "slot_id": case.slot_id,
        "input_before": before, "input_after": after,
        "input_before_sha256": canonical_hash(before),
        "input_after_sha256": canonical_hash(after),
        "expected_disposition": case.expected_disposition,
        "case_hash_role": "v2_envelope_dedupe_only", "v2_case": case.to_dict(),
        "execution_evidence": False, "candidate_eligible": False,
    }
    _deterministic_receipt(receipt)
    return MutationProposal(
        surface_id=TOPOLOGY_SURFACE, mutator_id=TOPOLOGY_MUTATOR_ID,
        parameters={"operator_id": case.operator_id, "slot_id": case.slot_id,
                    "request": request, "seed": seed, "input_layer_receipts": [receipt]},
        changes={"topology_program": after}, input_before={"topology_program": before},
        input_after={"topology_program": after}, application_phase="compile_time",
        applicability_result=_status(case.expected_disposition),
        proposal_id=f"topology-{index + 1:02d}-{case.operator_id}",
    )


def make_topology_plan(program, mutation_requests, *, rng_seed=0):
    """Return a core MutationPlan, not a bundle; apply 1..4 offline v2 layers.

    Each request has operator_id and optional slot_id/context.  Layer i uses
    seed rng_seed+i.  Guard negatives are terminal, explicitly rejected inputs.
    Receipts live at operations[i].parameters['input_layer_receipts'][0].
    """
    integer(rng_seed, "rng_seed")
    if not isinstance(mutation_requests, (list, tuple)) or not 1 <= len(mutation_requests) <= 4:
        raise ValueError("topology mutation_requests must contain 1..4 layers")
    program, _ = _program(program)
    contract = topology_contract()
    parent = {"topology_program": program}
    current, proposals = snapshot(parent), []
    for index, request in enumerate(mutation_requests):
        proposal = mutate_topology(
            current, contract.surfaces[0], make_rng(rng_seed + index),
            {"rng_seed": rng_seed, "layer_index": index, "mutation_request": request})
        proposals.append(proposal)
        if proposal.applicability_status == "rejected" and index + 1 < len(mutation_requests):
            raise ValueError("topology chain continues after a deliberate guard negative")
        current = snapshot(proposal.input_after)
    _validate_topology_proposals(parent, proposals, contract, rng_seed,
                                 RNG_ALGORITHM, len(proposals))
    plan = compose(parent, proposals, contract, rng_seed=rng_seed,
                   chain_length=len(proposals))
    validate_topology_plan(plan)
    return plan


def _layer(parameters, index, rng_seed):
    if not isinstance(parameters, dict) or set(parameters) != {
            "operator_id", "slot_id", "request", "seed", "input_layer_receipts"}:
        raise ValueError("topology operation parameters mismatch")
    seed = rng_seed + index
    if type(parameters["seed"]) is not int or parameters["seed"] != seed:
        raise ValueError("topology operation seed mismatch")
    request = _request(parameters["request"], seed)
    if request != parameters["request"]:
        raise ValueError("topology request is not normalized")
    receipts = parameters["input_layer_receipts"]
    if not isinstance(receipts, list) or len(receipts) != 1:
        raise ValueError("topology operation needs exactly one input layer receipt")
    receipt = receipts[0]
    if not isinstance(receipt, dict) or set(receipt) != {
            "schema", "layer_index", "input_path", "program_schema", "seed", "operator_id", "slot_id", "input_before",
            "input_after", "input_before_sha256", "input_after_sha256", "expected_disposition",
            "case_hash_role", "v2_case", "execution_evidence", "candidate_eligible"}:
        raise ValueError("topology input layer receipt fields mismatch")
    if receipt["input_path"] != "topology_program" or receipt["program_schema"] != topology_model.SCHEMA:
        raise ValueError("topology receipt declared program path/schema mismatch")
    if receipt["schema"] != TOPOLOGY_LAYER_SCHEMA or type(receipt["layer_index"]) is not int \
            or receipt["layer_index"] != index or type(receipt["seed"]) is not int \
            or receipt["seed"] != seed:
        raise ValueError("topology receipt order/seed mismatch")
    if receipt["execution_evidence"] is not False or receipt["candidate_eligible"] is not False:
        raise ValueError("topology input receipt cannot claim execution/candidate eligibility")
    if receipt["case_hash_role"] != "v2_envelope_dedupe_only":
        raise ValueError("topology v2 case hash must remain dedupe only")
    for key in ("operator_id", "slot_id"):
        requested = request.get(key)
        actual = receipt[key].split("#", 1)[0] if key == "slot_id" and isinstance(receipt[key], str) else receipt[key]
        if receipt[key] != parameters[key] or (key in request and requested != actual):
            raise ValueError("topology operator/slot mismatch")
    expected = receipt["expected_disposition"]
    before, _ = _program(receipt["input_before"])
    resolved = receipt["slot_id"]
    if not isinstance(resolved, str):
        raise ValueError("topology resolved slot must be a string")
    base, _, index_text = resolved.partition("#")
    declared = v2.surface_by_id("S1.topology").slot_by_id.get(base)
    if declared is None or receipt["operator_id"] not in declared.operators:
        raise ValueError("topology resolved slot is not declared by operator")
    if "[*]" in declared.input_path:
        collection = input_model.get_path(before, declared.input_path.split("[*]", 1)[0])
        if not index_text.isdecimal() or str(int(index_text)) != index_text \
                or not 0 <= int(index_text) < len(collection):
            raise ValueError("topology resolved wildcard index mismatch")
    elif "#" in resolved:
        raise ValueError("topology non-wildcard slot cannot have an index")
    after, validation = _program(receipt["input_after"], expected=expected,
                                 operator_id=receipt["operator_id"])
    if receipt["input_before_sha256"] != canonical_hash(before) \
            or receipt["input_after_sha256"] != canonical_hash(after):
        raise ValueError("topology raw-document hash mismatch")
    if before == after:
        raise ValueError("topology layer has no content change")
    if validation.get("guard_disposition") in {"invalid_input", "guard_violation"} \
            and expected != "expect_guard_reject":
        raise ValueError("topology guard negative lost its disposition")
    case = snapshot(receipt["v2_case"])
    if not isinstance(case, dict) or case.get("rng_algorithm") != RNG_ALGORITHM \
            or case.get("execution_evidence") is not False or case.get("candidate_eligible") is not False:
        raise ValueError("topology v2 case metadata mismatch")
    case.pop("rng_algorithm"); case.pop("execution_evidence"); case.pop("candidate_eligible")
    try:
        typed_case = v2.MutationCase(**case)
    except TypeError as exc:
        raise ValueError("topology v2 case fields mismatch") from exc
    if typed_case.surface_id != "S1.topology" or typed_case.operator_id != receipt["operator_id"] \
            or typed_case.slot_id != receipt["slot_id"] or type(typed_case.seed) is not int \
            or typed_case.seed != seed or typed_case.input != after \
            or typed_case.expected_disposition != expected \
            or typed_case.input_before_sha256 != v2._content_hash("S1.topology", before) \
            or typed_case.content_sha256 != v2._content_hash("S1.topology", after):
        raise ValueError("topology v2 case identity/hash mismatch")
    provenance = typed_case.provenance
    domain = v2.surface_by_id("S1.topology")
    expected_provenance = {
        "generator": "r2f.modules.s1.fuzz_mutators_v2",
        "generator_version": v2.GENERATOR_VERSION, "authored": False,
        "derived_from": "parent_input", "parent_sha256": typed_case.input_before_sha256,
        "operator_id": typed_case.operator_id,
        "chain": [{"operator_id": typed_case.operator_id, "slot_id": typed_case.slot_id,
                   "parameters": {"seed": seed, **request["context"].get("parameters", {})}}],
        "seed": seed, "rng_algorithm": RNG_ALGORITHM, "surface_version": domain.version,
        "domain_sha256": input_model.content_hash(domain.to_dict()),
    }
    if provenance != expected_provenance or type(provenance.get("seed")) is not int:
        raise ValueError("topology v2 provenance seed/identity mismatch")
    _deterministic_receipt(receipt)
    return receipt


def _validate_topology_proposals(parent, proposals, contract, rng_seed, rng_algorithm, chain_length):
    if rng_algorithm != RNG_ALGORITHM or chain_length != len(proposals) \
            or not 1 <= len(proposals) <= MUTATION_CHAIN_MAX_LENGTH:
        raise ValueError("topology proposals require an explicit bounded chain and RNG")
    if contract.to_dict() != topology_contract().to_dict():
        raise ValueError("topology proposal contract mismatch")
    current = snapshot(parent)
    for index, proposal in enumerate(proposals):
        receipt = _layer(proposal.parameters, index, rng_seed)
        if proposal.surface_id != TOPOLOGY_SURFACE or proposal.mutator_id != TOPOLOGY_MUTATOR_ID \
                or proposal.override or proposal.runtime_windows \
                or proposal.application_phase != "compile_time" \
                or proposal.proposal_id != f"topology-{index + 1:02d}-{receipt['operator_id']}" \
                or proposal.applicability_result != _status(receipt["expected_disposition"]):
            raise ValueError("topology proposal identity/status mismatch")
        before, after = ({"topology_program": receipt[name]} for name in ("input_before", "input_after"))
        if proposal.input_before != current or current != before \
                or proposal.input_after != after or proposal.changes != after:
            raise ValueError("topology proposal before/after chain mismatch")
        if receipt["expected_disposition"] == "expect_guard_reject" and index + 1 < len(proposals):
            raise ValueError("topology proposals continue after guard negative")
        current = after


def validate_topology_plan(plan):
    """Validate serialized content and seeds, NEVER call a mutation algorithm."""
    # Generic core validation is module-independent; this explicit entry point
    # adds S1 semantics. Providers/runtime must call it after deserialization.
    if not isinstance(plan, MutationPlan):
        raise ValueError("topology validator requires a core MutationPlan")
    plan.validate(contract=topology_contract())
    integer(plan.rng_seed, "rng_seed")
    if plan.rng_algorithm != RNG_ALGORITHM or set(plan.final_config) != {"topology_program"} \
            or plan.runtime_windows or not 1 <= len(plan.operations) <= MUTATION_CHAIN_MAX_LENGTH:
        raise ValueError("topology plan shape/RNG/budget mismatch")
    if plan.chain is None or plan.chain["chain_length"] != len(plan.operations):
        raise ValueError("topology plan requires its explicit 1..4 layer chain")
    plan._validate_chain(plan.chain)
    if plan.contract_hash != topology_contract().contract_hash():
        raise ValueError("topology plan extension contract_hash mismatch")
    previous = None
    for index, operation in enumerate(plan.operations):
        receipt = _layer(operation.parameters, index, plan.rng_seed)
        before, after = ({"topology_program": receipt[name]} for name in ("input_before", "input_after"))
        if operation.surface_id != TOPOLOGY_SURFACE or operation.mutator_id != TOPOLOGY_MUTATOR_ID \
                or operation.application_phase != "compile_time" \
                or operation.op_id != f"topology-{index + 1:02d}-{receipt['operator_id']}" \
                or operation.applicability_result != _status(receipt["expected_disposition"]):
            raise ValueError("topology operation identity/status mismatch")
        if (previous is not None and previous != before) \
                or operation.input_before_sha256 != canonical_hash(before) \
                or operation.input_after_sha256 != canonical_hash(after) \
                or (index == 0 and plan.parent_content_sha256 != canonical_hash(before)):
            raise ValueError("topology operation content/hash chain mismatch")
        if receipt["expected_disposition"] == "expect_guard_reject" and index + 1 < len(plan.operations):
            raise ValueError("topology plan continues after guard negative")
        previous = after
    if plan.final_config != previous:
        raise ValueError("topology final_config differs from last receipt")
    return True


def topology_input_layer_receipts(plan):
    """Detached deterministic receipts for existing effective-input assembly."""
    validate_topology_plan(plan)
    return snapshot([op.parameters["input_layer_receipts"][0] for op in plan.operations])
