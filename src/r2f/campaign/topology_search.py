"""Explicit topology requests for the shared input frontier. No execution or queue."""
from copy import deepcopy
from r2f.core.evidence import canonical_hash
from r2f.modules.s1 import fuzz_mutators_v2 as v2
from r2f.modules.s1.topology_input import make_topology_plan, topology_contract
from r2f.modules.s1.topology_targets import TOPOLOGY_TARGET, TOPOLOGY_SURFACE

SCHEMA = "r2f.topology-search/v1"
FRONTIER_SCHEMA = "r2f.input-search-frontier/topology-v1"
# Only selectors actually consumed by existing handlers. No generic catalog class aliases.
SELECTORS = {
    "endpoint.count": {"direction": {"add", "remove"}},
    "action.insert": {}, "action.delete": {}, "action.swap": {},
    "generation.action": {"variant": {"next", "stale"}},
    "queue.depth": {"boundary_class": {"min", "one", "max", "neighbor", "zero", "out_of_range_high"}},
    "param.boundary": {"boundary_class": {"min", "one", "max", "neighbor", "zero", "out_of_range_high"}},
    "param.enum": {},
    "relation.pair": {"pair": {"RR", "RB", "BB", "BR"}},
    "timing.shift": {"offset_sec": None},
    "lease.value": {"special_class": {"zero", "finite", "infinite"}},
    "filter.expr": {"filter_variant": {"valid", "invalid", "empty"}},
}


# Opt-in only: the first-slice schema, selectors and descriptor bytes stay intact.
OPERATORS_SCHEMA = "r2f.topology-search/operators-v1"
_ENV_KEYS = {"backend_available", "transport_ready", "discovery_enabled",
             "filter_support", "filter_validity"}
OPERATOR_SELECTORS = dict(SELECTORS, **{
    "guard.dependency": {}, "guard.parameter": {}, "guard.structure": {},
    "guard.environment": {"key": _ENV_KEYS | {"type_matching"}},
    "env.health": {"key": _ENV_KEYS}, "env.resource": {"key": _ENV_KEYS},
    # No delay_sec: topology consumes the declared QoS step, not wire delay.
    "env.delay": {}, "env.lease_expiry": {}, "env.lease_refresh": {},
    "param.enum": {"enum_value": {"best_effort", "reliable", "automatic",
                                    "manual_by_participant", "manual_by_topic"}},
    "param.boundary": {"boundary_class": SELECTORS["param.boundary"]["boundary_class"]
                                        | {"infinite"}},
})

def _pair_limitation(op, slot, expanded):
    if v2.OPERATOR_SPECS[op].accepted_kinds and slot.kind not in v2.OPERATOR_SPECS[op].accepted_kinds:
        return "operator_slot_kind_mismatch"
    if expanded and op in {"env.lease_expiry", "env.lease_refresh"} and slot.kind != "parameter_lease":
        return "handler_requires_parameter_lease"
    return None

def _sample_scope(op):
    if op == "env.delay":
        return "declared_qos_step_not_wire_delay"
    if op in {"env.lease_expiry", "env.lease_refresh"}:
        return "declared_lease_value_not_lifecycle"
    if op.startswith("guard."):
        return "generation_guard_only_not_execution_rejection"
    if op in {"env.health", "env.resource"}:
        return "declared_environment_not_measured_health"
    return "finite_deterministic_handler_sample_not_endpoint_exhaustive"

def descriptor(value, operators):
    if isinstance(value, dict) and value.get("schema") == CHAINS_SCHEMA:
        return _chains_descriptor(value, operators)
    if not isinstance(value, dict) or set(value) != {"schema", "samples"} or value["schema"] not in {SCHEMA, OPERATORS_SCHEMA}:
        raise ValueError("explicit topology_search schema required")
    expanded = value["schema"] == OPERATORS_SCHEMA
    selector_table = OPERATOR_SELECTORS if expanded else SELECTORS
    declared = {op for slot in v2.surface_by_id("S1.topology").slots for op in slot.operators}
    if not isinstance(operators, list) or not operators or any(not isinstance(op, str) or op not in declared for op in operators) or len(operators) != len(set(operators)):
        raise ValueError("invalid topology operator whitelist")
    samples = value["samples"]
    if not isinstance(samples, list) or not samples or len(samples) > 256:
        raise ValueError("topology_search requires 1..256 explicit samples")
    surface = v2.surface_by_id("S1.topology")
    pairs = {(op, slot.slot_id) for slot in surface.slots for op in slot.operators}
    seen = set()
    for sample in samples:
        if not isinstance(sample, dict) or set(sample) != {"operator_id", "slot_id", "class_id", "context"}:
            raise ValueError("topology sample requires operator/slot/class/context")
        op, slot = sample["operator_id"], sample["slot_id"]
        if (op, slot) not in pairs or op not in operators or op not in selector_table:
            raise ValueError("topology sample undeclared or handler_not_supported:" + str(op))
        if not isinstance(sample["class_id"], str) or not sample["class_id"] or not isinstance(sample["context"], dict):
            raise ValueError("invalid topology sample class/context")
        slot_spec = next(s for s in surface.slots if s.slot_id == slot)
        if expanded and _pair_limitation(op, slot_spec, True):
            raise ValueError("topology sample not_applicable:" + _pair_limitation(op, slot_spec, True))
        selectors = selector_table[op]
        for key, val in sample["context"].items():
            if key not in selectors or (selectors[key] is not None and (not isinstance(val, str) or val not in selectors[key])):
                raise ValueError("topology selector_not_consumed_or_unsupported:" + key)
            if key == "offset_sec" and (type(val) not in (int, float) or not 0 <= val <= 300):
                raise ValueError("invalid topology offset_sec")
        if expanded and "boundary_class" in sample["context"] and sample["context"]["boundary_class"] not in slot_spec.boundary_classes:
            raise ValueError("topology boundary_class not declared for slot")
        if expanded and "enum_value" in sample["context"] and sample["context"]["enum_value"] not in slot_spec.domain.get("values", []):
            raise ValueError("topology enum_value not declared for slot")
        if expanded and not selectors and sample["class_id"] != "handler_rng":
            raise ValueError("no-selector sample requires class_id=handler_rng; not boundary coverage")
        if expanded and op == "param.enum" and not sample["context"] and sample["class_id"] != "handler_rng":
            raise ValueError("enum RNG sample requires class_id=handler_rng")
        if selectors and not sample["context"] and not (expanded and op == "param.enum"):
            raise ValueError("topology parameter class requires consumed selector")
        identity = (op, slot, sample["class_id"])
        if identity in seen:
            raise ValueError("duplicate topology sample class")
        seen.add(identity)
    body = {"schema": value["schema"], "sampler": "explicit-consumed-context/operators-v1" if expanded else "explicit-consumed-context/v1",
            "target_id": TOPOLOGY_TARGET, "target_revision": 0, "contract_hash": topology_contract().contract_hash(),
            "operator_whitelist": sorted(operators), "delegate_manifest_sha256": topology_contract().surfaces[0].domain["delegate_manifest_sha256"],
            "surface_sha256": canonical_hash(surface.to_dict()),
            "samples": sorted(deepcopy(samples), key=lambda s: (s["operator_id"], s["slot_id"], s["class_id"]))}
    return dict(body, descriptor_sha256=canonical_hash(body))

def validate_descriptor(value):
    if not isinstance(value, dict):
        raise ValueError("missing topology descriptor")
    expected = descriptor({"schema": value.get("schema"), "samples": value.get("samples"),
                           **({"chain_length": value.get("chain_length")} if is_chain(value) else {})},
                          value.get("operator_whitelist", []))
    if value != expected:
        raise ValueError("topology descriptor/model drift")
    return expected

def enumerate_tasks(parent, desc):
    validate_descriptor(desc)
    if is_chain(desc):
        return _enumerate_chains(desc)
    expanded = desc["schema"] == OPERATORS_SCHEMA
    selector_table = OPERATOR_SELECTORS if expanded else SELECTORS
    tasks, ledger = [], []
    for slot in v2.surface_by_id("S1.topology").slots:
        for op in sorted(slot.operators):
            reason = _pair_limitation(op, slot, expanded)
            if reason is not None:
                pass
            elif op not in desc["operator_whitelist"]:
                reason = "operator_not_requested"
            elif op not in selector_table:
                reason = "handler_not_supported_in_first_slice"
            samples = [s for s in desc["samples"] if s["operator_id"] == op and s["slot_id"] == slot.slot_id]
            if reason is None and not samples:
                reason = "no_explicit_consumed_class"
            ledger.append({"operator_id": op, "slot_id": slot.slot_id, "status": "not_applicable" if reason else "applicable",
                           "reason": reason, "classes": [s["class_id"] for s in samples]})
            if expanded:
                ledger[-1]["sampling_scope"] = _sample_scope(op)
            if reason is None:
                for sample in samples:
                    request = {k: deepcopy(sample[k]) for k in ("operator_id", "slot_id", "context")}
                    tasks.append({"task_kind": "topology_request", "operator_id": op, "slot_id": slot.slot_id,
                                  "class_id": sample["class_id"], "mutation_request": request,
                                  "descriptor_sha256": desc["descriptor_sha256"]})
    return sorted(tasks, key=lambda t: (t["operator_id"], t["slot_id"], t["class_id"])), ledger

def validate_task(task, parent, desc):
    candidates, _ = enumerate_tasks(parent, desc)
    fields = (("task_kind", "chain_schema", "class_id", "layer_samples", "mutation_requests",
               "declared_layer_count", "descriptor_sha256") if is_chain(desc) else
              ("task_kind", "operator_id", "slot_id", "class_id", "mutation_request", "descriptor_sha256"))
    if not any(all(task.get(k) == candidate[k] for k in fields) for candidate in candidates):
        raise ValueError("topology task differs from explicit model request")
    if task["parent_content_sha256"] != parent["content_sha256"]:
        raise ValueError("topology task parent mismatch")

def generate(parent, task, desc):
    validate_task(task, parent, desc)
    # The previous generation's final config is the only submitted parent.
    requests = task_requests(task)
    try:
        return make_topology_plan(parent["final_config"]["topology_program"],
                                  requests, rng_seed=task["rng_seed"])
    except ValueError as exc:
        if not is_chain(desc):
            raise
        # Prefixes diagnose a failed full request only. Never return a shortened
        # plan or count an unattempted suffix as generated/executable.
        raise ChainGenerationRejected(str(exc), rejection_accounting(parent, task)) from exc


CHAINS_SCHEMA = "r2f.topology-search/chains-v1"
CHAINS_FRONTIER_SCHEMA = "r2f.input-search-frontier/topology-chains-v1"


def is_chain(value):
    return isinstance(value, dict) and value.get("schema") == CHAINS_SCHEMA


def validate_chain_length(profile, desc):
    length = profile.get("chain_length")
    if is_chain(desc):
        if type(length) is not int or length not in (2, 3, 4) or length != desc["chain_length"]:
            raise ValueError("topology chain_length must equal explicit chains schema length 2..4")
    elif type(length) is not int or length != 1:
        raise ValueError("topology_search first slice requires chain_length=1")


def _chains_descriptor(value, operators):
    if set(value) != {"schema", "chain_length", "samples"}:
        raise ValueError("topology chains schema requires explicit chain_length/samples")
    length = value["chain_length"]
    if type(length) is not int or length not in (2, 3, 4):
        raise ValueError("topology explicit chain_length must be 2..4")
    samples = value["samples"]
    if not isinstance(samples, list) or not 1 <= len(samples) <= 64:
        raise ValueError("topology chains require 1..64 declared sequences, not Cartesian expansion")
    seen = set()
    layer_classes = {}
    # Validate each layer with the existing expanded consumed-selector model;
    # duplicate requests/classes at different positions are legal, order matters.
    for sample in samples:
        if not isinstance(sample, dict) or set(sample) != {"class_id", "layers"}:
            raise ValueError("topology chain sample requires class_id and ordered layers")
        if not isinstance(sample["class_id"], str) or not sample["class_id"] or sample["class_id"] in seen:
            raise ValueError("invalid/duplicate topology chain class_id")
        seen.add(sample["class_id"])
        if not isinstance(sample["layers"], list) or len(sample["layers"]) not in (2, 3, 4):
            raise ValueError("topology chain sample requires an exact ordered 2..4 layers")
        for layer in sample["layers"]:
            descriptor({"schema": OPERATORS_SCHEMA, "samples": [layer]}, operators)
            key = (layer["operator_id"], layer["slot_id"], layer["class_id"])
            if key in layer_classes and layer_classes[key] != layer["context"]:
                raise ValueError("topology chain class has conflicting consumed context")
            layer_classes[key] = deepcopy(layer["context"])
    if not any(len(sample["layers"]) == length for sample in samples):
        raise ValueError("topology profile chain_length selects no declared sequence")
    # Reuse the exact authoritative target/domain bindings. This descriptor is
    # a new schema, not a conversion of a pre-existing single checkpoint.
    binding = descriptor({"schema": OPERATORS_SCHEMA, "samples": [samples[0]["layers"][0]]}, operators)
    body = {k: deepcopy(v) for k, v in binding.items() if k not in {"schema", "sampler", "samples", "descriptor_sha256"}}
    body.update(schema=CHAINS_SCHEMA, sampler="explicit-ordered-requests/v1", chain_length=length,
                samples=sorted(deepcopy(samples), key=lambda item: item["class_id"]))
    return dict(body, descriptor_sha256=canonical_hash(body))


def _enumerate_chains(desc):
    # Keep every declaration in the 31-pair ledger, including absent classes and
    # handler/kind constraints. Pair applicability is not sequence execution.
    layers = []
    seen = set()
    selected = [sample for sample in desc["samples"] if len(sample["layers"]) == desc["chain_length"]]
    for sample in selected:
        for layer in sample["layers"]:
            key = (layer["operator_id"], layer["slot_id"], layer["class_id"])
            if key not in seen:
                seen.add(key)
                layers.append(layer)
    single = descriptor({"schema": OPERATORS_SCHEMA, "samples": layers}, desc["operator_whitelist"])
    _, ledger = enumerate_tasks({}, single)
    tasks = []
    for sample in selected:
        tasks.append({"task_kind": "topology_request", "chain_schema": CHAINS_SCHEMA,
                      "class_id": sample["class_id"], "layer_samples": deepcopy(sample["layers"]),
                      "mutation_requests": [{k: deepcopy(layer[k]) for k in ("operator_id", "slot_id", "context")}
                                            for layer in sample["layers"]],
                      "declared_layer_count": desc["chain_length"],
                      "descriptor_sha256": desc["descriptor_sha256"]})
    return tasks, ledger


def task_requests(task):
    return deepcopy(task["mutation_requests"] if task.get("chain_schema") == CHAINS_SCHEMA
                    else [task["mutation_request"]])


def selection_metadata(selection):
    keys = (("task_kind", "chain_schema", "class_id", "layer_samples", "mutation_requests",
             "declared_layer_count", "descriptor_sha256") if selection.get("chain_schema") == CHAINS_SCHEMA
            else ("task_kind", "operator_id", "slot_id", "class_id", "mutation_request", "descriptor_sha256"))
    return {k: deepcopy(selection[k]) for k in keys}


def plan_accounting(plan, declared):
    from r2f.core.mutation_plan import mutation_plan_noop_reason, is_unwindowed_compile_time_noop
    operations = plan.operations
    executable = [op for op in operations if op.applicability_status not in {"rejected", "unsupported"}]
    active = [op for op in executable if op.applicability_status not in {"no_op", "overridden"}
              and not is_unwindowed_compile_time_noop(op, runtime_windows=plan.runtime_windows)]
    noop_reason = mutation_plan_noop_reason(plan)
    guard = any(op.applicability_status == "rejected" or
                getattr(op, "applicability_result", {}).get("expected_disposition") == "expect_guard_reject"
                for op in operations)
    return {"schema": "r2f.topology-chain-accounting/v1", "declared_layers": declared,
            "planned_layers": len(operations), "executable_layers": len(executable),
            "effective_layers": 0 if noop_reason or guard else len(active),
            "overridden_layers": sum(op.applicability_status == "overridden" for op in operations),
            "noop_reason": noop_reason, "count_scope": "input_generation_only_not_carrier_hit_or_semantic_effect",
            "execution_evidence": False, "candidate_eligible": False}


class ChainGenerationRejected(ValueError):
    def __init__(self, reason, accounting):
        super().__init__(reason)
        self.accounting = accounting


def rejection_accounting(parent, task):
    requests = task_requests(task)
    program = parent["final_config"]["topology_program"]
    prefix, failure = None, None
    for length in range(1, len(requests) + 1):
        try:
            candidate = make_topology_plan(program, requests[:length], rng_seed=task["rng_seed"])
        except ValueError as exc:
            failure = {"layer_index": length - 1, "request": requests[length - 1], "reason": str(exc)}
            break
        prefix = candidate
        if candidate.operations[-1].applicability_status == "rejected":
            failure = {"layer_index": length - 1, "request": requests[length - 1],
                       "reason": "topology chain continues after a deliberate guard negative",
                       "guard_terminal": True}
            break
    if prefix is None:
        result = {"schema": "r2f.topology-chain-accounting/v1", "declared_layers": len(requests),
                  "planned_layers": 0, "executable_layers": 0, "effective_layers": 0,
                  "overridden_layers": 0, "noop_reason": None,
                  "count_scope": "input_generation_only_not_carrier_hit_or_semantic_effect",
                  "execution_evidence": False, "candidate_eligible": False}
    else:
        result = plan_accounting(prefix, len(requests))
    # A diagnostic prefix is not an admitted/effective full-chain input.
    result["prefix_effective_layers"] = result["effective_layers"]
    result["effective_layers"] = 0
    result.update(full_sequence_rejected=True, failure=failure,
                  prefix_diagnostic_only=True, prefix_plan=prefix.to_dict() if prefix else None,
                  attempted_layers=(failure["layer_index"] + 1 if failure else len(requests)),
                  unattempted_suffix=requests[(failure["layer_index"] + 1 if failure else len(requests)):])
    return result


def sequence_applicability(desc):
    return [{"class_id": sample["class_id"], "declared_layers": len(sample["layers"]),
             "status": "applicable" if len(sample["layers"]) == desc["chain_length"] else "not_applicable",
             "reason": None if len(sample["layers"]) == desc["chain_length"] else "profile_chain_length_not_selected"}
            for sample in desc["samples"]]
