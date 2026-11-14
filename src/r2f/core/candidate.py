"""Append-only lifecycle audit of already observed conformance anomalies."""

from dataclasses import dataclass, field
from enum import Enum

from ._json import integer, nonempty, sha256_digest, snapshot
from .evidence import canonical_hash, verify_raw
from .execution import validate_execution
from .mutation import MutationReceipt
from .oracle import TriageResult
from .replay import ReplayStatus, conformance_signature, validate_replay_attempt
from .state_machine import utc_now


def _identity_members(value, field):
    """Identity containers may not disguise absent members as present evidence."""
    if value is None:
        raise ValueError(f"{field} contains a null identity member")
    if isinstance(value, str):
        nonempty(value, field)
    elif isinstance(value, (dict, list)):
        if not value:
            raise ValueError(f"{field} contains an empty identity container")
        members = value.items() if isinstance(value, dict) else enumerate(value)
        for key, member in members:
            _identity_members(member, f"{field}.{key}")


def _endpoint_pair(value, field):
    if not isinstance(value, dict) or not {"writer", "reader"}.issubset(value):
        raise ValueError(f"{field} requires writer and reader identity records")
    _identity_members(value, field)
    for role in ("writer", "reader"):
        endpoint = value[role]
        if not isinstance(endpoint, dict):
            raise ValueError(f"{field}.{role} must be an identity record")
        nonempty(endpoint.get("id"), f"{field}.{role}.id")
        generation = endpoint.get("generation")
        if isinstance(generation, str):
            nonempty(generation, f"{field}.{role}.generation")
        else:
            integer(generation, f"{field}.{role}.generation")
    return value


def _validate_observation_identity(row):
    facts = row.get("typed_facts", {})
    if not isinstance(facts, dict):
        raise ValueError("typed_facts must be a dict for identity binding")
    present = False
    for field in ("endpoint_identity", "instrumentation_identity"):
        if field not in row:
            continue
        present = True
        identity = row[field]
        _identity_members(identity, field)
        if isinstance(identity, str):
            # Retain the existing opaque-string contract. If endpoint facts are
            # available, the supported pair string must name those exact ids.
            if field == "endpoint_identity" and any(role in facts for role in ("writer", "reader")):
                pair = _endpoint_pair({role: facts.get(role) for role in ("writer", "reader")}, "typed_facts")
                expected = f"{pair['writer']['id']}/{pair['reader']['id']}"
                if identity != expected:
                    raise ValueError("endpoint_identity string differs from typed_facts endpoints")
        elif field == "endpoint_identity":
            pair = _endpoint_pair(identity, field)
            fact_pair = _endpoint_pair({role: facts.get(role) for role in ("writer", "reader")}, "typed_facts")
            for role in ("writer", "reader"):
                for key, value in pair[role].items():
                    if key not in fact_pair[role] or canonical_hash(value) != canonical_hash(fact_pair[role][key]):
                        raise ValueError(f"endpoint_identity.{role}.{key} differs from typed_facts")
        elif isinstance(identity, dict):
            nonempty(identity.get("id"), field + ".id")
        else:
            raise ValueError(f"{field} must be a nonempty string or an identity record")
        if field in facts and canonical_hash(identity) != canonical_hash(facts[field]):
            raise ValueError(f"{field} differs from typed_facts.{field}")
    if not present:
        raise ValueError("observed requires stable endpoint/instrumentation identity")


class CandidateStatus(str, Enum):
    OBSERVED = "observed"
    ADMITTED = "admitted"
    TRIAGED = "triaged"
    CONFIRMED = "confirmed"
    DEFERRED = "deferred"
    REJECTED = "rejected"


@dataclass
class CandidateLifecycle:
    candidate_id: str
    evidence: dict
    candidate_status: str = field(default="observed", init=False)
    replay_status: str = field(default="not_run", init=False)
    _history: list = field(default_factory=list, init=False, repr=False)
    _replay_attempts: list = field(default_factory=list, init=False, repr=False)
    _triage: dict | None = field(default=None, init=False, repr=False)
    _confirmation: dict | None = field(default=None, init=False, repr=False)
    _non_replayable: dict | None = field(default=None, init=False, repr=False)
    _blackbox_validation: str = field(default="not_run", init=False, repr=False)

    def __post_init__(self):
        nonempty(self.candidate_id, "candidate_id")
        self.evidence = snapshot(self.evidence)
        self._validate_observed()
        self._audit("observed")

    def _audit(self, event, **details):
        self._history.append({"event": event, "candidate_status": self.candidate_status,
                              "timestamp": utc_now(), **snapshot(details)})

    def _validate_observed(self):
        row = self.evidence
        for key in ("run_id", "target_id", "rmw", "timestamp", "raw_path"):
            nonempty(row.get(key), key)
        integer(row.get("target_revision"), "target_revision")
        _validate_observation_identity(row)
        if row.get("candidate_source") not in {"instrumentation", "blackbox", "differential", "control"}:
            raise ValueError("invalid candidate_source")
        if row.get("evidence_tier") not in {"instrumented", "blackbox", "blackbox_replayed", "verified"}:
            raise ValueError("invalid evidence_tier")
        verify_raw(row["raw_path"], row.get("raw_sha256"))

    def _validate_admission(self):
        self._validate_observed()
        row = self.evidence
        if row.get("arm") == "negative" or row.get("control_kind") or row.get("candidate_source") == "control":
            raise ValueError("control rows cannot enter SUT candidates")
        if row.get("responsibility_layer") in {"harness", "mutation"} or row.get("reason_code") in {
            "reach_or_planning", "mutation_ineffective", "tool_error", "environment_or_process"
        }:
            raise ValueError("candidate is attributed to execution/harness failure")
        if row.get("execution_stage") not in {"effect_observed", "judged"}:
            raise ValueError("admission requires actual effect observation")
        validate_execution(row)
        integer(row.get("repeat"), "repeat")
        if not (row.get("observed_effect") or row.get("typed_facts")):
            raise ValueError("candidate requires an observed effect or typed fact")
        if not (row.get("rule_id") or row.get("internal_contract") or row.get("differential_assertion")):
            raise ValueError("candidate requires a rule/contract/assertion")
        if not isinstance(row.get("provenance"), dict) or not row["provenance"]:
            raise ValueError("candidate requires provenance")
        if row["arm"] in {"mutated", "negative"}:
            receipt = MutationReceipt.from_record(row)
            if not all((receipt.carrier_admitted, receipt.configured, receipt.applied, receipt.hit)):
                raise ValueError("mutation candidate requires admitted/configured/applied/hit")
            sha256_digest(row.get("effective_input_sha256"), "effective_input_sha256")
        if row["candidate_source"] == "instrumentation":
            provenance = row["provenance"]
            for key in ("instrumentation_point", "event_time", "control"):
                nonempty(provenance.get(key), f"provenance.{key}")
            if not (provenance.get("source_identity") or provenance.get("binary_identity")):
                raise ValueError("instrumentation requires source or binary identity")

    def admit(self):
        if self.candidate_status != "observed":
            raise ValueError("admission is only valid from observed")
        self._validate_admission()
        self.candidate_status = "admitted"
        self._audit("admitted")
        return self

    def triage(self, result):
        if self.candidate_status not in {"admitted", "triaged", "deferred"}:
            raise ValueError("triage requires an admitted candidate")
        if isinstance(result, dict):
            result = TriageResult(**result)
        if not isinstance(result, TriageResult):
            raise ValueError("triage requires TriageResult or its plain dict")
        # Invalid execution evidence must never be elevated through manual triage.
        self._validate_admission()
        self._triage = result.to_dict()
        disposition = result.triage_disposition
        self.candidate_status = {"candidate": "triaged", "deferred": "deferred",
                                 "rejected": "rejected", "pass": "rejected"}[disposition]
        self._audit("triaged", **self._triage)
        return self

    def record_replay(self, status, *, failure_reason="", **metadata):
        attempt = snapshot(metadata)
        attempt.update(status=ReplayStatus(status).value, failure_reason=failure_reason,
                       timestamp=utc_now(), attempt_id=f"attempt-{len(self._replay_attempts) + 1}")
        validate_replay_attempt(attempt)
        self._replay_attempts.append(attempt)
        self.replay_status = attempt["status"]
        self._audit("replay_attempt", attempt_id=attempt["attempt_id"], replay_status=self.replay_status)
        return self

    def mark_non_replayable(self, disposition, reason):
        nonempty(disposition, "non_replayable.disposition")
        nonempty(reason, "non_replayable.reason")
        self._non_replayable = {"disposition": disposition, "reason": reason, "timestamp": utc_now()}
        self._audit("non_replayable", **self._non_replayable)
        return self

    def confirm(self, *, adjudicator_id, adjudication_method, confirmation_basis="", reason=""):
        if self.candidate_status != "triaged" or not self._triage or self._triage["responsibility_layer"] != "sut":
            raise ValueError("confirmation requires SUT triage")
        nonempty(adjudicator_id, "adjudicator_id")
        if adjudication_method not in {"single_agent", "peer_review", "rule_engine"}:
            raise ValueError("invalid adjudication_method")
        matching_replay = False
        if self.replay_status == "pass" and self._replay_attempts:
            attempt = self._replay_attempts[-1]
            expected = conformance_signature(self.evidence)
            original_identity = {key: self.evidence.get(key) for key in (
                "run_id", "target_id", "target_revision", "rmw", "arm", "repeat", "raw_sha256", "effective_input_sha256")}
            matching_replay = (expected is not None and expected["oracle_verdict"] == "fail"
                               and attempt.get("comparison_basis") == "original_execution_signature"
                               and attempt.get("comparison_matches_original") is True
                               and canonical_hash(attempt.get("original_signature")) == canonical_hash(expected)
                               and canonical_hash(attempt.get("replay_signature")) == canonical_hash(expected)
                               and canonical_hash(attempt.get("original_execution_identity")) == canonical_hash(original_identity))
        if not matching_replay:
            nonempty(confirmation_basis, "confirmation_basis without a matching anomaly replay")
            nonempty(reason, "confirmation evidence reason")
        self._confirmation = {"adjudicator_id": adjudicator_id, "adjudication_method": adjudication_method,
                              "adjudicated_at": utc_now(), "confirmation_basis": confirmation_basis or "conformance_signature_match",
                              "reason": reason}
        self.candidate_status = "confirmed"
        self._audit("confirmed", **self._confirmation)
        return self

    def record_blackbox_validation(self, status):
        if status not in {"not_run", "pass", "fail", "inconclusive"}:
            raise ValueError("invalid blackbox_validation")
        self._blackbox_validation = status
        self._audit("blackbox_validation", status=status)
        return self

    def validate(self, *, require_disposition=False):
        self._validate_observed()
        if self.candidate_status != "observed":
            self._validate_admission()
        if require_disposition and self.candidate_status in {"admitted", "triaged", "deferred", "confirmed"}:
            if not self._replay_attempts and not self._non_replayable:
                raise ValueError("candidate requires a replay attempt or named non_replayable disposition")
        if self.candidate_status == "confirmed" and self.replay_status != "pass":
            if not self._confirmation or not self._confirmation["confirmation_basis"]:
                raise ValueError("confirmed without replay requires named evidence basis")
        return True

    def to_dict(self):
        unconfirmed = self.candidate_status in {"admitted", "triaged", "deferred"} and self.replay_status != "pass"
        return {"candidate_id": self.candidate_id, "candidate_status": self.candidate_status,
                "evidence": snapshot(self.evidence), "candidate_source": self.evidence["candidate_source"],
                "evidence_tier": self.evidence["evidence_tier"], "replay_status": self.replay_status,
                "replay_attempts": snapshot(self._replay_attempts), "non_replayable": snapshot(self._non_replayable),
                "blackbox_validation": self._blackbox_validation, "derived_labels": ["replay_unconfirmed"] if unconfirmed else [],
                "history": snapshot(self._history), **snapshot(self._triage or {}), **snapshot(self._confirmation or {})}
