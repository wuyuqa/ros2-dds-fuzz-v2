"""Canonical execution stages and cumulative evidence gates."""

from dataclasses import dataclass, field
from enum import Enum

from ._json import integer, nonempty, sha256_digest, snapshot
from .evidence import verify_raw
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


def validate_fault_control_execution(record):
    """Validate the negative-arm harness receipt, separately from mutation hit.

    raw_event_ref accepts the runtime events' nonnegative integer index, or a
    nonempty textual raw event reference. Raw identity is checked by the
    cumulative execution gate; the runtime adapter binds the referenced event.
    """
    if record.get("arm") != "negative" or record.get("control_kind") != "fault":
        raise ValueError("fault-control execution requires negative arm and fault control_kind")
    if record.get("execution_stage") == "mutation_applied":
        raise ValueError("fault-control execution cannot enter mutation_applied")
    receipt = record.get("fault_control_receipt")
    if not isinstance(receipt, dict):
        raise ValueError("fault_control_receipt is required")
    if receipt.get("fault_id") != "late_endpoint_delivery_omission":
        raise ValueError("unsupported fault_control_receipt.fault_id")
    if receipt.get("applied") is not True:
        raise ValueError("fault control requires applied=true")
    integer(receipt.get("omitted_sequence"), "fault_control_receipt.omitted_sequence")
    reference = receipt.get("raw_event_ref")
    if type(reference) is int:
        integer(reference, "fault_control_receipt.raw_event_ref")
    else:
        nonempty(reference, "fault_control_receipt.raw_event_ref")
    if receipt.get("responsibility_layer") != "harness" or receipt.get("candidate_eligible") is not False:
        raise ValueError("fault control must declare harness responsibility and candidate_eligible=false")
    mutation = MutationReceipt.from_record(record)
    if mutation.applied or mutation.hit or mutation.effect_observed:
        raise ValueError("fault control must not claim mutation applied/hit/effect_observed")
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
