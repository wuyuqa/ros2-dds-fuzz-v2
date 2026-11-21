"""Append-only lifecycle audit of already observed conformance anomalies."""

import re
import json
import os
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum

from ._json import integer, nonempty, sha256_digest, snapshot
from .evidence import canonical_hash, verify_raw, raw_sha256
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



TOPOLOGY_IDENTITY_SCHEMA = "r2f.topology-candidate-identity/v1"
_TOPOLOGY_FACTS_SCHEMA = "r2f.s1-topology-facts/v1"
_TOPOLOGY_TARGET = "discovery.endpoint_topology.fuzz_v1"


def topology_observation_identity(row):
    """Project existing N-endpoint evidence; never invent a representative pair.

    This is an observation identity, not an admission/qualification receipt.
    Raw verification remains mandatory at lifecycle intake.
    """
    facts = row.get("typed_facts", {})
    return snapshot({"schema": TOPOLOGY_IDENTITY_SCHEMA, "run_id": row.get("run_id"),
                     "raw": facts.get("raw"),
                     "endpoint_lineage": facts.get("endpoint_lineage"),
                     "pairs": facts.get("pairs")})


def _validate_topology_identity(row, identity):
    facts = row.get("typed_facts", {})
    if row.get("target_id") != _TOPOLOGY_TARGET or facts.get("schema") != _TOPOLOGY_FACTS_SCHEMA:
        raise ValueError("topology identity requires exact target and typed facts schema")
    if canonical_hash(identity) != canonical_hash(topology_observation_identity(row)):
        raise ValueError("topology identity differs from run/raw/typed facts")
    raw = identity.get("raw")
    if not isinstance(raw, dict) or raw != {"path": row.get("raw_path"), "sha256": row.get("raw_sha256")}:
        raise ValueError("topology raw identity differs from observation")
    verify_raw(raw["path"], raw["sha256"])
    try:
        with open(raw["path"], encoding="utf-8") as stream:
            events = [json.loads(line) for line in stream if line.strip()]
    except (OSError, ValueError) as exc:
        raise ValueError("topology raw events unreadable") from exc
    if not events or any(not isinstance(e, dict) or e.get("run_id") != row.get("run_id")
                         or type(e.get("event_index")) is not int or e["event_index"] != i
                         for i, e in enumerate(events)):
        raise ValueError("topology raw run/event identity mismatch")

    def reference(ref):
        if not isinstance(ref, dict) or ref.get("path") != raw["path"] or ref.get("sha256") != raw["sha256"]:
            raise ValueError("topology proof reference crosses raw identity")
        index = ref.get("event_index")
        if type(index) is not int or not 0 <= index < len(events):
            raise ValueError("topology proof event index invalid")
        return events[index]

    program = (row.get("final_input") or {}).get("topology_program", {})
    declarations = program.get("endpoints", [])
    if not isinstance(declarations, list) or not declarations:
        raise ValueError("topology endpoint roles missing")
    roles = {}
    for endpoint in declarations:
        if not isinstance(endpoint, dict):
            raise ValueError("topology endpoint declaration invalid")
        logical_id, role = endpoint.get("logical_id"), endpoint.get("role")
        nonempty(logical_id, "topology logical_id")
        if logical_id in roles or role not in {"writer", "reader"}:
            raise ValueError("topology endpoint role missing or ambiguous")
        roles[logical_id] = role
    lineage = identity.get("endpoint_lineage")
    if not isinstance(lineage, list) or not lineage:
        raise ValueError("topology endpoint lineage missing")
    tokens, gids, generations = {}, set(), set()
    for endpoint in lineage:
        if not isinstance(endpoint, dict):
            raise ValueError("topology lineage record invalid")
        logical_id, generation = endpoint.get("logical_id"), endpoint.get("generation")
        if logical_id not in roles or type(generation) is not int or generation < 1:
            raise ValueError("topology lineage role/generation invalid")
        key = (logical_id, generation)
        if key in tokens:
            raise ValueError("topology duplicate endpoint generation")
        for name in ("endpoint_gid", "endpoint_generation"):
            nonempty(endpoint.get(name), "topology lineage." + name)
        for name in ("pid", "process_start_ticks"):
            if type(endpoint.get(name)) is not int or endpoint[name] < 1:
                raise ValueError("topology lineage process identity invalid")
        if endpoint["endpoint_gid"] in gids or endpoint["endpoint_generation"] in generations:
            raise ValueError("topology reused endpoint instance identity")
        gids.add(endpoint["endpoint_gid"])
        generations.add(endpoint["endpoint_generation"])
        event = reference(endpoint.get("creation_ref"))
        if event.get("event") != "endpoint_created" or any(
                event.get(name) != endpoint.get(name) for name in
                ("logical_id", "generation", "endpoint_generation", "pid", "process_start_ticks")):
            raise ValueError("topology lineage differs from raw creation identity")
        # A lineage summary names the final authenticated DDS incarnation, not
        # necessarily the creation GID. Keep the immutable process/origin check
        # above, then independently reopen the complete API/graph/transition/ACK
        # chain. Neither a changed GID alone nor omitted metadata permits a
        # legacy creation-only interpretation of an actual raw transition.
        incarnation_declared = any(name in endpoint for name in
                                   ("incarnation_ordinal", "identity_transition_ref"))
        raw_transition = any(e.get("event") == "endpoint_identity_transition"
                             and e.get("logical_id") == logical_id
                             and e.get("generation") == generation for e in events)
        if incarnation_declared or raw_transition:
            from r2f.runtime.endpoint_incarnation import endpoint_identity_at
            current = endpoint_identity_at(events, event)
            if (type(endpoint.get("incarnation_ordinal")) is not int
                    or "identity_transition_ref" not in endpoint
                    or any(canonical_hash(endpoint.get(name)) != canonical_hash(current.get(name))
                           for name in ("endpoint_gid", "incarnation_ordinal", "identity_transition_ref"))):
                raise ValueError("topology lineage differs from authenticated current incarnation")
        elif event.get("endpoint_gid") != endpoint.get("endpoint_gid"):
            raise ValueError("topology lineage differs from raw creation identity")
        if not re.fullmatch(roles[logical_id] + r"[0-9]*", str(event.get("role", ""))):
            raise ValueError("topology endpoint role differs from raw creation")
        tokens[key] = endpoint
    pairs = identity.get("pairs")
    if not isinstance(pairs, list) or not pairs:
        raise ValueError("topology requires concrete directed pairs, not total counts")
    seen = set()
    for pair in pairs:
        if not isinstance(pair, dict):
            raise ValueError("topology pair invalid")
        keys = []
        for role in ("writer", "reader"):
            logical_id, generation = pair.get(role), pair.get(role + "_generation")
            if not isinstance(logical_id, str) or type(generation) is not int:
                raise ValueError("topology pair role/generation missing")
            key = (logical_id, generation)
            if roles.get(logical_id) != role or key not in tokens:
                raise ValueError("topology pair crosses endpoint role/generation")
            keys.append(key)
        edge = tuple(keys)
        if edge in seen:
            raise ValueError("topology directed pair ambiguous")
        seen.add(edge)
        proofs = pair.get("proofs")
        if not isinstance(proofs, dict) or not proofs:
            raise ValueError("topology pair proof references missing")
        refs = []
        for proof in proofs.values():
            if not isinstance(proof, dict) or not isinstance(proof.get("refs"), list):
                raise ValueError("topology pair proof references invalid")
            refs.extend(proof["refs"])
        if not refs:
            raise ValueError("topology pair proof references missing")
        def proof_references(value):
            if isinstance(value, dict):
                if "event_index" in value or "path" in value:
                    reference(value)
                else:
                    for member in value.values():
                        proof_references(member)
            elif isinstance(value, list):
                for member in value:
                    proof_references(member)
        proof_references(pair)
        # Evidence must at least name both actual instances; a count is not an edge.
        for key in keys:
            if tokens[key]["creation_ref"] not in refs:
                raise ValueError("topology pair lacks endpoint creation proof")
    if row.get("oracle_verdict") == "fail":
        # Bind the oracle's explicit anomaly objects to existing directed pairs.
        # Do not derive an anomaly from N counts, or change the primary verdict.
        closure = (row.get("oracle") or {}).get("closure", {}).get("pairs", [])
        applicable = False
        for subject in closure:
            if not isinstance(subject, dict):
                raise ValueError("topology anomaly subject invalid")
            key = ((subject.get("writer"), subject.get("writer_generation")),
                   (subject.get("reader"), subject.get("reader_generation")))
            if key not in seen:
                raise ValueError("topology anomaly subject crosses directed pair identity")
            pair = next(p for p in pairs if (
                (p["writer"], p["writer_generation"]), (p["reader"], p["reader_generation"])) == key)
            # These are identity applicability checks on typed oracle objects,
            # not a replacement oracle or a re-evaluation of its verdict.
            missing = pair.get("proofs", {}).get("delivery_closure", {}).get("detail", {}).get("missing", [])
            if (subject.get("missing") and subject["missing"] == missing
                    or pair.get("compatibility") == "incompatible" and (
                        pair.get("observed_match") in {"bidirectional", "writer_only", "reader_only"}
                        or bool(pair.get("communication_witnesses")))):
                applicable = True
        if not applicable:
            raise ValueError("topology anomaly requires concrete applicable pair objects")


def _validate_topology_admission(row):
    """Respect declared qualification denials; identity never promotes them."""
    facts, oracle = row.get("typed_facts", {}), row.get("oracle", {})
    if any(value is not True for value in (row.get("candidate_eligible"),
                                           facts.get("candidate_eligible"), oracle.get("candidate_eligible"))):
        raise ValueError("topology candidate qualification not granted")
    if row.get("oracle_verdict") != "fail" or oracle.get("verdict") != "fail":
        raise ValueError("topology candidate requires primary fail")
    if (facts.get("fixture") is not False or facts.get("control_excluded") is not False
            or facts.get("tool_error") is not False or oracle.get("diagnostic_only") is not False
            or any(facts.get(key) is not True for key in
                   ("execution_evidence", "source_provenance_qualified", "environment_healthy",
                    "process_healthy", "observation_complete", "endpoint_identity_complete"))):
        raise ValueError("topology candidate requires qualified complete natural evidence")


def _validate_observation_identity(row):
    facts = row.get("typed_facts", {})
    if not isinstance(facts, dict):
        raise ValueError("typed_facts must be a dict for identity binding")
    if facts.get("schema") == _TOPOLOGY_FACTS_SCHEMA and (
            not isinstance(row.get("endpoint_identity"), dict)
            or row["endpoint_identity"].get("schema") != TOPOLOGY_IDENTITY_SCHEMA):
        raise ValueError("topology observation requires versioned N-endpoint identity")
    present = False
    for field in ("endpoint_identity", "instrumentation_identity"):
        if field not in row:
            continue
        present = True
        identity = row[field]
        if (field == "endpoint_identity" and isinstance(identity, dict)
                and identity.get("schema") == TOPOLOGY_IDENTITY_SCHEMA):
            _validate_topology_identity(row, identity)
            continue
        if field == "endpoint_identity" and facts.get("schema") == _TOPOLOGY_FACTS_SCHEMA:
            raise ValueError("topology observation requires versioned N-endpoint identity")
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


REVIEW_EVIDENCE_KEYS = frozenset({
    "source", "raw", "capsule", "replay", "oracle", "control", "summary",
})

_SOURCE_REF = re.compile(r"^(?:[0-9a-f]{7,64}|[A-Za-z0-9_.\-/]{2,})$")


def _existing_reference(value, field):
    """A review must point at real local artefacts, not invented labels."""
    nonempty(value, f"review.evidence_refs.{field}")
    if not os.path.exists(value):
        raise ValueError(f"review evidence reference does not exist: {field}={value}")
    return os.path.abspath(value)


HUMAN_ADJUDICATION_SCHEMA = "r2f.s1-human-adjudication/v1"


def candidate_execution_identity(row):
    """Bind a decision to the exact original saved execution, not a projection."""
    return {
        **{key: row.get(key) for key in (
            "run_id", "cell_id", "target_id", "target_revision", "rule_id", "rmw",
            "effective_input_sha256", "raw_path", "raw_sha256")},
        "source": row.get("source_identity") or row.get("source_manifest") or row.get("provenance"),
        "execution_sha256": canonical_hash(row),
    }


def validate_human_record(record, original_row, *, human_authority=None):
    """Integrity is not authentication. Only a trusted host verifier grants authority.

    The verifier is supplied out of band by an authenticated human UI/instruction
    adapter. Never obtain it from records, reports, or an agent's claimed identity.
    There is intentionally no default verifier; offline fixtures supply their own.
    """
    if not isinstance(record, dict) or not isinstance(original_row, dict):
        raise ValueError("human_record_or_original_invalid")
    if record.get("schema_version") != HUMAN_ADJUDICATION_SCHEMA:
        raise ValueError("human_schema_unknown")
    for key in ("record_id", "human_id", "reason", "instruction_ref", "instruction_sha256"):
        nonempty(record.get(key), "human_" + key)
    if record.get("origin") not in {"human_operation", "user_instruction"}:
        raise ValueError("human_origin_invalid")
    if record.get("action") not in {"confirm", "revoke"}:
        raise ValueError("human_action_invalid")
    try:
        when = datetime.fromisoformat(record["recorded_at"])
        if when.utcoffset() is None:
            raise ValueError("timezone required")
    except (KeyError, ValueError, TypeError) as exc:
        raise ValueError("human_recorded_at_invalid") from exc
    if record.get("record_sha256") != canonical_hash({
            k: v for k, v in record.items() if k != "record_sha256"}):
        raise ValueError("human_record_digest_mismatch")
    if (record.get("execution_identity") != candidate_execution_identity(original_row)
            or record.get("candidate_id") != f"{original_row.get('run_id')}:{original_row.get('cell_id')}"):
        raise ValueError("human_execution_identity_mismatch")
    verify_raw(original_row.get("raw_path"), original_row.get("raw_sha256"))
    verify_raw(record["instruction_ref"], record["instruction_sha256"])
    try:
        verified = callable(human_authority) and human_authority(snapshot(record)) is True
    except Exception as exc:
        raise ValueError("human_authority_unverified") from exc
    if not verified:
        raise ValueError("human_authority_unverified")
    return True


def validate_human_history(record, previous):
    """Return False for an identical ID; reject conflicts, forks and silent edits."""
    duplicate = next((r for r in previous if r["record_id"] == record["record_id"]), None)
    if duplicate is not None:
        if duplicate != record:
            raise ValueError("human_record_id_conflict")
        return False
    history = [r for r in previous if r["candidate_id"] == record["candidate_id"]]
    if history:
        if record.get("supersedes_record_id") != history[-1]["record_id"]:
            raise ValueError("human_revision_requires_current_supersedes")
        nonempty(record.get("revision_reason"), "human_revision_reason")
        if record["execution_identity"] != history[-1]["execution_identity"]:
            raise ValueError("human_supersedes_execution_mismatch")
        if datetime.fromisoformat(record["recorded_at"]) < datetime.fromisoformat(history[-1]["recorded_at"]):
            raise ValueError("human_revision_time_precedes_parent")
    elif (record.get("supersedes_record_id") is not None
          or record.get("revision_reason") is not None or record["action"] != "confirm"):
        raise ValueError("human_supersedes_identity_unknown")
    return True


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
    _reviews: list = field(default_factory=list, init=False, repr=False)
    _human_records: list = field(default_factory=list, init=False, repr=False)
    _human_original: dict | None = field(default=None, init=False, repr=False)
    _human_authority: object = field(default=None, init=False, repr=False)

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
        if (isinstance(row.get("endpoint_identity"), dict)
                and row["endpoint_identity"].get("schema") == TOPOLOGY_IDENTITY_SCHEMA):
            _validate_topology_admission(row)
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

    def record_review(self, *, reviewer_id, conclusion, evidence_refs, independent_of=None,
                      notes="", attribution=None):
        """Append an independent review opinion.  This never confirms a candidate.

        ``conclusion`` is one of supported/rejected/needs_evidence.  A review
        must name a reviewer that is not one of ``independent_of`` (the trigger
        chain/oracle authors) and must reference concrete source, raw and
        capsule artefacts.  Review metadata is evidence for adjudication, not a
        lifecycle transition.
        """
        nonempty(reviewer_id, "review.reviewer_id")
        if conclusion not in {"supported", "rejected", "needs_evidence"}:
            raise ValueError("invalid review conclusion")
        separations = independent_of if independent_of is not None else []
        if not isinstance(separations, (list, tuple)) or any(
                not isinstance(item, str) or not item.strip() for item in separations):
            raise ValueError("review.independent_of must be a list of identities")
        if reviewer_id in separations:
            raise ValueError("reviewer cannot review its own trigger chain/oracle work")
        if not isinstance(evidence_refs, dict) or not evidence_refs:
            raise ValueError("review requires source/raw/capsule evidence references")
        unknown = set(evidence_refs) - REVIEW_EVIDENCE_KEYS
        if unknown:
            raise ValueError("review evidence has unknown references: " + ",".join(sorted(unknown)))
        resolved = {}
        for key in ("source", "raw", "capsule"):
            value = evidence_refs.get(key)
            nonempty(value, f"review.evidence_refs.{key}")
            if not _SOURCE_REF.fullmatch(value):
                raise ValueError(f"review evidence reference is not a path/commit: {key}")
            resolved[key] = _existing_reference(value, key)
        # Optional extra references must also point at real artefacts when they
        # look like local paths; a bare label cannot stand in for evidence.
        for key, value in evidence_refs.items():
            if key in resolved:
                continue
            nonempty(value, f"review.evidence_refs.{key}")
            if isinstance(value, str) and ("/" in value or os.sep in value):
                resolved[key] = _existing_reference(value, key)
        review = {"reviewer_id": reviewer_id, "conclusion": conclusion,
                  "evidence_refs": resolved,
                  "independent_of": list(separations),
                  "notes": notes, "reviewed_at": utc_now(),
                  "review_id": f"review-{len(self._reviews) + 1}"}
        if attribution is not None:
            if attribution not in {"sut_candidate", "tool", "environment", "allowed", "unknown"}:
                raise ValueError("invalid review attribution")
            review["attribution"] = attribution
            review["evidence_sha256"] = {key: raw_sha256(value) for key, value in resolved.items()}
        self._reviews.append(review)
        self._audit("reviewed", review_id=review["review_id"], conclusion=conclusion,
                    reviewer_id=reviewer_id)
        return self

    def _independent_supporting_review(self):
        """Most recent independent supporting review, if any.

        The reviewer and the adjudicator are different roles: a review is an
        input to adjudication, not the adjudication itself.  Any independent
        reviewer with concrete references qualifies; adjudication remains a
        separate, explicit act by ``confirm``.
        """
        for review in reversed(self._reviews):
            if review["conclusion"] == "supported":
                return review
        return None

    def confirm(self, *, adjudicator_id, adjudication_method, confirmation_basis="", reason="",
                require_review=False):
        if self.candidate_status != "triaged" or not self._triage or self._triage["responsibility_layer"] != "sut":
            raise ValueError("confirmation requires SUT triage")
        nonempty(adjudicator_id, "adjudicator_id")
        if adjudication_method not in {"single_agent", "peer_review", "rule_engine"}:
            raise ValueError("invalid adjudication_method")
        mandatory_review = require_review or self.evidence.get("review_required") is True
        review = self._independent_supporting_review() if self._reviews else None
        if mandatory_review and review is None:
            raise ValueError("confirmation requires an independent supporting review "
                             "with source/raw/capsule references")
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
        if review is not None:
            self._confirmation["review_id"] = review["review_id"]
            self._confirmation["reviewed_evidence"] = snapshot(review["evidence_refs"])
        self.candidate_status = "confirmed"
        self._audit("confirmed", **self._confirmation)
        return self

    def _validate_human_binding(self, record, original):
        if self.candidate_id != record["candidate_id"] or any(
                self.evidence.get(key) != value for key, value in original.items()
                if key not in {"timestamp", "candidate_source", "evidence_tier", "endpoint_identity"}):
            raise ValueError("human_lifecycle_execution_mismatch")

    def _validate_human_records(self, original, human_authority):
        previous = []
        for record in self._human_records:
            validate_human_record(record, original, human_authority=human_authority)
            self._validate_human_binding(record, original)
            validate_human_history(record, previous)
            previous.append(record)

    def _human_supporting_review(self, record, *, historical=False):
        reviewers = set()
        for review in reversed(self._reviews):
            if not historical and review["reviewer_id"] in reviewers:
                continue
            reviewers.add(review["reviewer_id"])
            if (review["conclusion"] == "supported" and review.get("attribution") == "sut_candidate"
                    and review["independent_of"] and review["reviewer_id"] != record["human_id"]
                    and review["evidence_refs"]["raw"] == os.path.abspath(self.evidence["raw_path"])):
                try:
                    for key, ref in review["evidence_refs"].items():
                        verify_raw(ref, review.get("evidence_sha256", {}).get(key))
                except (OSError, ValueError, TypeError):
                    continue
                return True
        return False

    def _validate_human_execution(self, original):
        self._validate_admission()
        if (self.candidate_status not in {"triaged", "confirmed"} or not self._triage
                or self._triage["responsibility_layer"] != "sut"
                or self._triage["triage_disposition"] != "candidate"):
            raise ValueError("human_confirmation_requires_sut_triage")
        if (("candidate_eligible" in self.evidence and self.evidence["candidate_eligible"] is not True)
                or original.get("candidate_source") == "control"
                or self.evidence.get("oracle_verdict") != "fail"
                or self.evidence.get("actual_run") is not True
                or self.evidence.get("observer_complete") is not True
                or self.evidence.get("tool_error") is not False
                or self.evidence.get("observer_dropped") is not False):
            raise ValueError("human_confirmation_requires_healthy_execution")

    def restore_human_history(self, records, *, original_row, human_authority=None):
        """Restore authenticated history without granting current confirmation.

        Historical support permits retaining a withdrawn analysis. A new confirm
        must still enter through record_human_adjudication's current-review gate.
        """
        if self._human_records:
            raise ValueError("human_history_restore_requires_empty_history")
        previous = []
        for record in records:
            validate_human_record(record, original_row, human_authority=human_authority)
            self._validate_human_binding(record, original_row)
            if validate_human_history(record, previous):
                if record["action"] == "confirm" and not self._human_supporting_review(record, historical=True):
                    raise ValueError("human_confirmation_requires_independent_supporting_review")
                previous.append(snapshot(record))
        self._validate_human_execution(original_row)
        self._human_records = previous
        self._human_original = snapshot(original_row)
        self._human_authority = human_authority
        for record in previous:
            self._audit("human_history_restored", record_id=record["record_id"], action=record["action"])
        return self

    def record_human_adjudication(self, record, *, original_row=None, human_authority=None):
        """Apply a separately authenticated human decision to this lifecycle."""
        original = self.evidence if original_row is None else original_row
        self._validate_human_records(original, human_authority)
        validate_human_record(record, original, human_authority=human_authority)
        self._validate_human_binding(record, original)
        is_new = validate_human_history(record, self._human_records)
        self._validate_human_execution(original)
        if record["action"] == "confirm" and not self._human_supporting_review(record):
            raise ValueError("human_confirmation_requires_independent_supporting_review")
        if not is_new:
            if record == self._human_records[-1]:
                self.candidate_status = "confirmed" if record["action"] == "confirm" or self._confirmation else "triaged"
            return self
        self.candidate_status = "confirmed" if record["action"] == "confirm" or self._confirmation else "triaged"
        self._human_original = snapshot(original)
        self._human_authority = human_authority
        self._human_records.append(snapshot(record))
        self._audit("human_adjudicated", record_id=record["record_id"], action=record["action"])
        return self

    def record_blackbox_validation(self, status):
        if status not in {"not_run", "pass", "fail", "inconclusive"}:
            raise ValueError("invalid blackbox_validation")
        self._blackbox_validation = status
        self._audit("blackbox_validation", status=status)
        return self

    def validate(self, *, require_disposition=False, require_review=False):
        self._validate_observed()
        if self.candidate_status != "observed":
            self._validate_admission()
        if require_disposition and self.candidate_status in {"admitted", "triaged", "deferred", "confirmed"}:
            if not self._replay_attempts and not self._non_replayable:
                raise ValueError("candidate requires a replay attempt or named non_replayable disposition")
        self._validate_human_records(self._human_original, self._human_authority)
        human_confirmed = bool(self._human_records and self._human_records[-1]["action"] == "confirm")
        if human_confirmed:
            self.record_human_adjudication(self._human_records[-1], original_row=self._human_original,
                                           human_authority=self._human_authority)
        if self.candidate_status == "confirmed" and self.replay_status != "pass":
            if not human_confirmed and (not self._confirmation or not self._confirmation["confirmation_basis"]):
                raise ValueError("confirmed without replay requires named evidence basis")
        if require_review and self.candidate_status == "confirmed":
            if not self._independent_supporting_review():
                raise ValueError("confirmed candidate requires an independent supporting review "
                                 "with source/raw/capsule evidence")
        return True

    def to_dict(self):
        unconfirmed = self.candidate_status in {"admitted", "triaged", "deferred"} and self.replay_status != "pass"
        return {"candidate_id": self.candidate_id, "candidate_status": self.candidate_status,
                "evidence": snapshot(self.evidence), "candidate_source": self.evidence["candidate_source"],
                "evidence_tier": self.evidence["evidence_tier"], "replay_status": self.replay_status,
                "replay_attempts": snapshot(self._replay_attempts), "non_replayable": snapshot(self._non_replayable),
                "blackbox_validation": self._blackbox_validation, "derived_labels": ["replay_unconfirmed"] if unconfirmed else [],
                "reviews": snapshot(self._reviews),
                "human_adjudication": {"current": snapshot(self._human_records[-1]) if self._human_records else None,
                                       "history": snapshot(self._human_records)},
                "history": snapshot(self._history), **snapshot(self._triage or {}), **snapshot(self._confirmation or {})}
