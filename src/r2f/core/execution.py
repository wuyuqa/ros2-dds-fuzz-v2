"""Canonical execution stages and cumulative evidence gates."""

from dataclasses import dataclass, field
from enum import Enum

from ._json import integer, nonempty, sha256_digest, snapshot
from .evidence import canonical_hash, verify_raw
from .mutation import MutationReceipt
from .state_machine import StateCondition, utc_now


class ExecutionStage(str, Enum):
    MODELLED = "modelled"
    EXPRESSIBLE = "expressible"
    PLANNED = "planned"
    PRECONDITION_EXECUTED = "precondition_executed"
    REACHED = "reached"
    MUTATION_APPLIED = "mutation_applied"
    EFFECT_OBSERVED = "effect_observed"
    JUDGED = "judged"
    ABORTED = "aborted"


_STAGES = [stage for stage in ExecutionStage if stage != ExecutionStage.ABORTED]


def validate_neutral(record):
    if record.get("arm") not in {"baseline", "transparent"}:
        raise ValueError("neutral path is only available to baseline/transparent arms")
    receipt = record.get("neutral_receipt")
    if not isinstance(receipt, dict) or receipt.get("kind") not in {"neutral", "no_op"}:
        raise ValueError("neutral path requires a neutral/no_op audit receipt")
    nonempty(receipt.get("reason"), "neutral_receipt.reason")
    before = sha256_digest(receipt.get("before_input_sha256"), "before_input_sha256")
    after = sha256_digest(receipt.get("after_input_sha256"), "after_input_sha256")
    if before != after:
        raise ValueError("neutral path changed effective submitted input")
    mutation = MutationReceipt.from_record(record)
    if mutation.applied or mutation.hit or mutation.effect_observed:
        raise ValueError("neutral arms cannot claim execution mutation applied/hit/effect_observed")
    return True


_LEGACY_FAULT_TARGETS = frozenset({"discovery.late_endpoint", "discovery.late_join_delivery"})
_FAULT_RECEIPT_SCHEMA = "r2f.fault-control-receipt/v1"


def _validate_raw_ref(value, field):
    if type(value) is int:
        integer(value, field)
    else:
        nonempty(value, field)


def _declared_fault_contract(record):
    target = record.get("target")
    if not isinstance(target, dict):
        raise ValueError("fault-control execution requires a target manifest")
    nonempty(target.get("target_id"), "target.target_id")
    if record.get("target_id") != target["target_id"]:
        raise ValueError("fault-control target identity differs from manifest")
    controls = target.get("control_contract", {})
    if not isinstance(controls, dict):
        raise ValueError("target.control_contract must be a dict")
    contract = controls.get("fault", {})
    if not isinstance(contract, dict):
        raise ValueError("target.control_contract.fault must be a dict")
    return target, contract


def _validate_delivery_fault(receipt, contract):
    window = nonempty(contract.get("window"), "control_contract.fault.window")
    if receipt.get("window") != window:
        raise ValueError("fault receipt window differs from target declaration")
    integer(receipt.get("omitted_sequence"), "fault_control_receipt.omitted_sequence")
    refs = receipt.get("window_refs")
    if not isinstance(refs, dict):
        raise ValueError("delivery fault requires window_refs")
    opening, closing = refs.get("open_event_ref"), refs.get("close_event_ref")
    _validate_raw_ref(opening, "window_refs.open_event_ref")
    _validate_raw_ref(closing, "window_refs.close_event_ref")
    if type(opening) is int and type(closing) is int and opening >= closing:
        raise ValueError("delivery window open event must precede close event")


def _validate_projection_fault(receipt):
    nonempty(receipt.get("field_path"), "fault_control_receipt.field_path")
    source = receipt.get("projection_source")
    if not isinstance(source, str) or source not in {"builtin", "graph"}:
        raise ValueError("projection_source must identify builtin or graph")
    if "before" not in receipt or "after" not in receipt:
        raise ValueError("projection fault requires before and after values")
    if canonical_hash(receipt["before"]) == canonical_hash(receipt["after"]):
        raise ValueError("projection fault before and after must differ as typed JSON values")
    sources = receipt.get("original_sources")
    if not isinstance(sources, dict):
        raise ValueError("projection fault requires both original sources")
    for source in ("builtin", "graph"):
        original = sources.get(source)
        if not isinstance(original, dict):
            raise ValueError(f"original_sources.{source} is required")
        sha256_digest(original.get("raw_sha256"), f"original_sources.{source}.raw_sha256")
        _validate_raw_ref(original.get("raw_ref"), f"original_sources.{source}.raw_ref")
    _validate_raw_ref(receipt.get("controlled_projection_ref"), "controlled_projection_ref")


def validate_fault_control_execution(record):
    """Validate a negative harness receipt against its target's exact contract.

    Declared v1 receipts require fault_id/receipt_kind, plus a delivery window or
    the original projection-source references. Schema validation does not read
    source artifacts: the runtime adapter verifies hashes, event binding,
    chronology, closure and independent sources against unchanged original raw.

    The first two revision-0 targets retain their existing no-kind v0 receipt.
    They must explicitly declare the original fault id; this compatibility never
    accepts a missing declaration, different target, fault id or new receipt kind.
    """
    if record.get("arm") != "negative" or record.get("control_kind") != "fault":
        raise ValueError("fault-control execution requires negative arm and fault control_kind")
    if record.get("execution_stage") == "mutation_applied":
        raise ValueError("fault-control execution cannot enter mutation_applied")
    receipt = record.get("fault_control_receipt")
    if not isinstance(receipt, dict):
        raise ValueError("fault_control_receipt is required")
    if receipt.get("applied") is not True:
        raise ValueError("fault control requires applied=true")
    _validate_raw_ref(receipt.get("raw_event_ref"), "fault_control_receipt.raw_event_ref")
    if receipt.get("responsibility_layer") != "harness" or receipt.get("candidate_eligible") is not False:
        raise ValueError("fault control must declare harness responsibility and candidate_eligible=false")
    mutation = MutationReceipt.from_record(record)
    if mutation.applied or mutation.hit or mutation.effect_observed:
        raise ValueError("fault control must not claim mutation applied/hit/effect_observed")

    target, contract = _declared_fault_contract(record)
    if "receipt_kind" not in contract:
        legacy_id = "late_endpoint_delivery_omission"
        if (target["target_id"] not in _LEGACY_FAULT_TARGETS
                or type(target.get("target_revision")) is not int or target["target_revision"] != 0
                or contract.get("fault_id") != legacy_id
                or receipt.get("fault_id") != legacy_id
                or "receipt_kind" in receipt or "schema" in receipt):
            raise ValueError("legacy fault receipt is limited to the original two revision-0 target contracts")
        integer(receipt.get("omitted_sequence"), "fault_control_receipt.omitted_sequence")
        return True

    fault_id = nonempty(contract.get("fault_id"), "control_contract.fault.fault_id")
    kind = contract["receipt_kind"]
    if not isinstance(kind, str) or kind not in {"delivery_omission", "projection_field_mismatch"}:
        raise ValueError("unsupported declared fault receipt_kind")
    if contract.get("arm") != "negative" or contract.get("candidate_eligible") is not False:
        raise ValueError("declared fault control must be negative and candidate-ineligible")
    if receipt.get("schema") != _FAULT_RECEIPT_SCHEMA:
        raise ValueError("unsupported fault receipt schema")
    if receipt.get("fault_id") != fault_id or receipt.get("receipt_kind") != kind:
        raise ValueError("fault receipt id/kind differs from target declaration")
    if kind == "delivery_omission":
        _validate_delivery_fault(receipt, contract)
    else:
        _validate_projection_fault(receipt)
    return True


def _validate_steps(record):
    target = record["target"]
    steps = target.get("state_steps", [])
    receipts = record.get("precondition_receipts")
    if not isinstance(receipts, list) or len(receipts) != len(steps):
        raise ValueError("every state step requires one executed before/after receipt")
    previous = None
    for step, receipt in zip(steps, receipts):
        if not isinstance(receipt, dict) or receipt.get("step_id") != step["step_id"] or receipt.get("action") != step["action"]:
            raise ValueError("state receipt identity/order does not match plan")
        if receipt.get("status") != "executed":
            raise ValueError("state step was not executed successfully")
        for key in ("before", "after", "requires"):
            if not isinstance(receipt.get(key), dict):
                raise ValueError(f"state receipt missing {key}")
        nonempty(receipt.get("started_at"), "receipt.started_at")
        nonempty(receipt.get("finished_at"), "receipt.finished_at")
        if previous is not None and receipt["before"] != previous:
            raise ValueError("state receipt sequence is discontinuous")
        checks = {c["condition_id"]: StateCondition.from_dict(c).evaluate(receipt["before"])
                  for c in step.get("requires", [])}
        if receipt["requires"] != checks or any(value is not True for value in receipt["requires"].values()):
            raise ValueError("step requires did not evaluate to true")
        previous = receipt["after"]
    return previous if previous is not None else record.get("final_state", {})


def validate_execution(record, *, verify_raw_evidence=True):
    record = snapshot(record)
    if "stage" in record or "state_status" in record:
        raise ValueError("execution_stage is the only execution state field")
    try:
        stage = ExecutionStage(record["execution_stage"])
    except (ValueError, KeyError) as exc:
        raise ValueError("invalid execution_stage") from exc
    nonempty(record.get("run_id"), "run_id")
    target = record.get("target")
    if not isinstance(target, dict):
        raise ValueError("target manifest required")
    from .semantic_target import SemanticTarget
    manifest = SemanticTarget.from_dict(target)
    if record.get("target_id") != manifest.target_id or record.get("target_revision") != manifest.target_revision:
        raise ValueError("target identity differs from manifest")
    if record.get("arm") not in {"baseline", "transparent", "mutated", "negative"}:
        raise ValueError("unsupported arm")
    mutation = MutationReceipt.from_record(record)
    if record["arm"] in {"baseline", "transparent", "negative"} and (mutation.hit or mutation.applied):
        raise ValueError("baseline/transparent/negative cannot claim mutation applied/hit")
    # A receipt cannot turn ineffective compile-time input generation into an
    # applied mutation.  This is an admission rule, not a reinterpretation of
    # historical carrier raw evidence.  Neutral/fault controls are unchanged.
    if (record["arm"] == "mutated" and record.get("mutation_plan") is not None
            and (mutation.applied or mutation.hit or mutation.effect_observed
                 or stage in {ExecutionStage.MUTATION_APPLIED,
                              ExecutionStage.EFFECT_OBSERVED, ExecutionStage.JUDGED})):
        from .mutation_plan import MutationPlan, mutation_plan_noop_reason
        plan = MutationPlan.from_dict(record["mutation_plan"])
        plan.validate()
        noop_reason = mutation_plan_noop_reason(plan)
        if noop_reason is not None:
            raise ValueError("mutated execution requires effective input generation: " + noop_reason)
    if stage == ExecutionStage.ABORTED:
        nonempty(record.get("abort_reason"), "abort_reason")
        return True
    rank = _STAGES.index(stage)
    if rank >= 1 and record.get("adapter_accepted") is not True:
        raise ValueError("expressible requires actual adapter acceptance")
    if rank >= 2:
        if record.get("state_steps") != target["state_steps"]:
            raise ValueError("compiled state steps must match target manifest")
    final_state = _validate_steps(record) if rank >= 3 else {}
    if rank >= 4:
        if not all(StateCondition.from_dict(c).evaluate(final_state) for c in target["state_preconditions"]):
            raise ValueError("state preconditions were not reached")
    if stage == ExecutionStage.MUTATION_APPLIED or (rank >= 6 and record["arm"] == "mutated"):
        if record["arm"] in {"baseline", "transparent", "negative"} or not all((mutation.carrier_admitted, mutation.configured,
                                                                 mutation.applied, mutation.hit)):
            raise ValueError("mutation_applied requires admitted, configured, applied and hit")
    if rank >= 6:
        if record["arm"] in {"baseline", "transparent"}:
            validate_neutral(record)
        elif record["arm"] == "negative":
            validate_fault_control_execution(record)
        nonempty(record.get("raw_path"), "raw_path")
        sha256_digest(record.get("raw_sha256"), "raw_sha256")
        if verify_raw_evidence:
            verify_raw(record["raw_path"], record["raw_sha256"])
        if not (record.get("observed_effect") or record.get("typed_facts")):
            raise ValueError("effect_observed requires raw-backed effect or typed facts")
    if rank >= 7 and record.get("oracle_verdict") not in {"pass", "fail", "unknown", "not_applicable"}:
        raise ValueError("judged requires a four-state oracle verdict")
    return True


@dataclass
class ExecutionRecord:
    run_id: str
    scenario: object
    execution_stage: ExecutionStage = field(default=ExecutionStage.MODELLED, init=False)
    _data: dict = field(default_factory=dict, init=False, repr=False)
    _history: list = field(default_factory=list, init=False, repr=False)

    def __post_init__(self):
        nonempty(self.run_id, "run_id")
        self._data = self.scenario.to_dict()
        self._data.update(run_id=self.run_id, execution_stage="modelled",
                          mutation_receipt=MutationReceipt().to_dict())
        self._history = [{"execution_stage": "modelled", "timestamp": utc_now()}]
        validate_execution(self._data)

    def advance(self, stage, **evidence):
        stage = ExecutionStage(stage)
        if self.execution_stage in {ExecutionStage.ABORTED, ExecutionStage.JUDGED}:
            raise ValueError("execution has already terminated")
        if stage == ExecutionStage.ABORTED:
            return self.abort(evidence.get("abort_reason", ""))
        expected = _STAGES[_STAGES.index(self.execution_stage) + 1]
        neutral_jump = (self.execution_stage == ExecutionStage.REACHED and stage == ExecutionStage.EFFECT_OBSERVED
                        and self._data["arm"] in {"baseline", "transparent"})
        fault_jump = (self.execution_stage == ExecutionStage.REACHED and stage == ExecutionStage.EFFECT_OBSERVED
                      and self._data["arm"] == "negative")
        if stage != expected and not (neutral_jump or fault_jump):
            raise ValueError("execution stages cannot be skipped")
        identity = {"run_id", "target", "target_id", "target_revision", "rmw", "arm", "repeat", "seed",
                    "state_steps", "execution_stage", "stage_history", "parameters"}
        if identity.intersection(evidence):
            raise ValueError("execution identity/plan cannot be overwritten by transition evidence")
        proposal = dict(self._data, **snapshot(evidence), execution_stage=stage.value)
        validate_execution(proposal)
        self._data = proposal
        self.execution_stage = stage
        self._history.append({"execution_stage": stage.value, "timestamp": utc_now()})
        return self

    def abort(self, reason):
        if self.execution_stage in {ExecutionStage.ABORTED, ExecutionStage.JUDGED}:
            raise ValueError("execution has already terminated")
        nonempty(reason, "abort_reason")
        self.execution_stage = ExecutionStage.ABORTED
        self._data.update(execution_stage="aborted", abort_reason=reason)
        self._history.append({"execution_stage": "aborted", "timestamp": utc_now()})
        return self

    def to_dict(self):
        return dict(snapshot(self._data), stage_history=snapshot(self._history))
