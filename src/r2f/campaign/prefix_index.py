"""Raw-backed prefix history for continuous episodes.

A prefix is a real, observed starting state for an episode.  This module
indexes three kinds of prefix -- first creation, after network recovery and
after delete/recreate -- and never invents one from an abstract plan.  Records
must carry raw-backed receipts; an offline fixture is marked ``fixture=true``
and is excluded from live selection.

Selection is deterministic (coverage, hit rate, cost; stable id tiebreak) and
returns its provenance instead of randomly substituting an independent input.
Insert/replace/delete transformations are dependency-guarded plans: every
result is checked for closure and resource constraints before being accepted.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from copy import deepcopy
import math

from r2f.core._json import nonempty, snapshot
from r2f.core.evidence import canonical_hash
from r2f.core.state_signature import STATE_COMPONENTS
from .corpus import content_identity
from .episode import _safe_id, _sha as _episode_sha

SCHEMA = "r2f.s1-prefix-index/v1"
RECORD_SCHEMA = "r2f.s1-prefix-record/v1"
SELECTION_SCHEMA = "r2f.s1-prefix-selection/v1"
TRANSFORM_SCHEMA = "r2f.s1-prefix-transform/v1"
OUTCOME_SCHEMA = "r2f.s1-prefix-outcome/v1"

PREFIX_KINDS = frozenset({"first_creation", "after_network_recovery",
                          "after_delete_recreate"})
STRUCTURAL_KEYS = frozenset(set(STATE_COMPONENTS) | {
    "endpoint_generations", "writer_endpoint_generation", "reader_endpoint_generation",
    "writer_endpoint_gid", "reader_endpoint_gid", "topic", "message_type",
})
COUNT_ONLY_KEYS = frozenset({"count", "counts", "matched_count", "writer_count",
                             "reader_count", "publisher_count", "subscription_count"})
OPAQUE_COUNT_KEYS = frozenset({"count", "counts"})
REQUIRED_RECEIPT_ROLES = {
    "first_creation": frozenset({"writer_creation", "reader_creation"}),
    "after_network_recovery": frozenset({"network_restore", "fresh_writer_count",
                                         "fresh_reader_count", "fresh_graph",
                                         "postrestore_delivery"}),
    "after_delete_recreate": frozenset({"endpoint_delete", "endpoint_recreate"}),
}


class PrefixError(ValueError):
    """Invalid prefix record, selection or transformation."""


class PrefixHistoryError(PrefixError):
    """A prefix cannot be selected because its history is incomplete."""


def _sha(value, field="sha256"):
    try:
        return _episode_sha(value, field)
    except ValueError as exc:
        raise PrefixError(str(exc)) from exc


def _guard_name(value, field):
    nonempty(value, field)
    return value


@dataclass(frozen=True)
class StateFingerprint:
    """A state fingerprint that may never be only a delivery count.

    Components are canonical abstract state keys plus at least one structural
    observation (role, match, network phase or endpoint generation).  Counts
    are useful supporting diagnostics but never sufficient identity.
    """
    components: dict

    def __post_init__(self):
        components = snapshot(self.components)
        if not isinstance(components, dict) or not components:
            raise PrefixError("state fingerprint requires components")
        for key, value in components.items():
            if not isinstance(key, str) or not key:
                raise PrefixError("state fingerprint keys must be nonempty strings")
            if isinstance(value, bool):
                continue
            if isinstance(value, (int, float)):
                if not math.isfinite(float(value)):
                    raise PrefixError("state fingerprint numbers must be finite")
            elif isinstance(value, str):
                if not value:
                    raise PrefixError("state fingerprint strings must be nonempty")
            else:
                raise PrefixError("state fingerprint values must be scalar")
        structural = set(components) & STRUCTURAL_KEYS
        count_like = set(components) & (COUNT_ONLY_KEYS | {"count-only", "count_only"})
        if not structural:
            raise PrefixError("state fingerprint cannot be only count fields")
        if count_like == set(components):
            raise PrefixError("state fingerprint cannot be only count fields")
        object.__setattr__(self, "components", {key: components[key] for key in sorted(components)})

    def fingerprint_id(self):
        return canonical_hash({"schema": "r2f.s1-state-fingerprint/v1",
                               "components": self.components})

    def to_dict(self):
        return {"components": deepcopy(self.components),
                "fingerprint_id": self.fingerprint_id()}

    @classmethod
    def from_dict(cls, data):
        if not isinstance(data, dict) or set(data) != {"components", "fingerprint_id"}:
            raise PrefixError("state fingerprint fields are not exact")
        result = cls(deepcopy(data["components"]))
        if data["fingerprint_id"] != result.fingerprint_id():
            raise PrefixError("state fingerprint id mismatch")
        return result


def _validate_receipt(receipt, index):
    if not isinstance(receipt, dict):
        raise PrefixError(f"receipts[{index}] must be an object")
    role = _guard_name(receipt.get("role"), f"receipts[{index}].role")
    if not isinstance(role, str):
        raise PrefixError("receipt role must be a string")
    raw_path = receipt.get("raw_path")
    if not isinstance(raw_path, str) or not raw_path:
        raise PrefixError(f"receipts[{index}].raw_path is required")
    _sha(receipt.get("raw_sha256"), f"receipts[{index}].raw_sha256")
    event_index = receipt.get("event_index")
    if type(event_index) is not int or event_index < 0:
        raise PrefixError(f"receipts[{index}].event_index must be a nonnegative integer")
    if type(receipt.get("fixture", False)) is not bool:
        raise PrefixError("receipt fixture flag must be boolean")
    return deepcopy(receipt)


@dataclass
class PrefixRecord:
    prefix_id: str
    kind: str
    target_id: str
    target_revision: int
    rmw: str
    contract_hash: str
    state: StateFingerprint
    receipts: list
    provenance: dict
    cost: int = 0
    depends_on: list[str] = field(default_factory=list)
    resources: list[str] = field(default_factory=list)
    coverage: list[str] = field(default_factory=list)
    attempts: int = 0
    hits: int = 0
    fixture: bool = False

    def __post_init__(self):
        _safe_id(self.prefix_id, "prefix_id")
        if self.kind not in PREFIX_KINDS:
            raise PrefixError("unsupported prefix kind: " + repr(self.kind))
        nonempty(self.target_id, "target_id")
        if type(self.target_revision) is not int or self.target_revision < 0:
            raise PrefixError("target_revision must be a nonnegative integer")
        nonempty(self.rmw, "rmw")
        nonempty(self.contract_hash, "contract_hash")
        if not isinstance(self.state, StateFingerprint):
            raise PrefixError("prefix state must be a StateFingerprint")
        if not isinstance(self.receipts, list) or not self.receipts:
            raise PrefixError("prefix requires raw-backed receipts")
        self.receipts = [_validate_receipt(item, index)
                         for index, item in enumerate(self.receipts)]
        roles = {item.get("role") for item in self.receipts}
        required = REQUIRED_RECEIPT_ROLES[self.kind]
        if not required <= roles:
            missing = sorted(required - roles)
            raise PrefixError(f"{self.kind} prefix missing raw-backed role(s): " + ",".join(missing))
        self.provenance = snapshot(self.provenance)
        if not isinstance(self.provenance, dict) or not self.provenance:
            raise PrefixError("prefix provenance must be a nonempty object")
        for key in ("source_identity", "run_id", "raw_evidence"):
            if key not in self.provenance:
                raise PrefixError("prefix provenance requires " + key)
        if type(self.cost) is not int or self.cost < 0:
            raise PrefixError("prefix cost must be a nonnegative integer")
        for name in ("depends_on", "resources", "coverage"):
            value = getattr(self, name)
            if (not isinstance(value, list)
                    or any(not isinstance(item, str) or not item for item in value)
                    or len(set(value)) != len(value)):
                raise PrefixError(name + " must be a list of unique nonempty strings")
        if self.prefix_id in self.depends_on:
            raise PrefixError("prefix cannot depend on itself")
        for name in ("attempts", "hits"):
            value = getattr(self, name)
            if type(value) is not int or value < 0:
                raise PrefixError(name + " must be a nonnegative integer")
        if self.hits > self.attempts:
            raise PrefixError("prefix hits cannot exceed attempts")
        if type(self.fixture) is not bool:
            raise PrefixError("prefix fixture flag must be boolean")

    def hit_rate(self):
        return 0.0 if self.attempts == 0 else self.hits / self.attempts

    def selection_key(self):
        """Coverage first, then hit rate, then cheaper, then stable id."""
        return (-len(set(self.coverage)), -self.hit_rate(), self.cost, self.prefix_id)

    def to_dict(self):
        document = {
            "schema": RECORD_SCHEMA,
            "prefix_id": self.prefix_id,
            "kind": self.kind,
            "target_id": self.target_id,
            "target_revision": self.target_revision,
            "rmw": self.rmw,
            "contract_hash": self.contract_hash,
            "state": self.state.to_dict(),
            "receipts": deepcopy(self.receipts),
            "provenance": deepcopy(self.provenance),
            "cost": self.cost,
            "depends_on": list(self.depends_on),
            "resources": list(self.resources),
            "coverage": list(self.coverage),
            "attempts": self.attempts,
            "hits": self.hits,
            "fixture": self.fixture,
        }
        document["record_sha256"] = canonical_hash(document)
        return document

    @classmethod
    def from_dict(cls, data):
        if not isinstance(data, dict) or data.get("schema") != RECORD_SCHEMA:
            raise PrefixError("unsupported prefix record schema")
        received = data.get("record_sha256")
        if received != canonical_hash({k: v for k, v in data.items() if k != "record_sha256"}):
            raise PrefixError("prefix record hash mismatch")
        record = cls(prefix_id=data.get("prefix_id"), kind=data.get("kind"),
                     target_id=data.get("target_id"), target_revision=data.get("target_revision"),
                     rmw=data.get("rmw"), contract_hash=data.get("contract_hash"),
                     state=StateFingerprint.from_dict(data.get("state")),
                     receipts=deepcopy(data.get("receipts")), provenance=deepcopy(data.get("provenance")),
                     cost=data.get("cost", 0), depends_on=list(data.get("depends_on", [])),
                     resources=list(data.get("resources", [])), coverage=list(data.get("coverage", [])),
                     attempts=data.get("attempts", 0), hits=data.get("hits", 0),
                     fixture=data.get("fixture", False))
        if record.to_dict() != data:
            raise PrefixError("prefix record bytes are not canonical")
        return record


@dataclass
class PrefixIndex:
    """Append-only prefix records plus auditable selection and outcome logs."""
    records: dict = field(default_factory=dict)
    selection_log: list = field(default_factory=list)
    outcome_log: list = field(default_factory=list)

    def add(self, record):
        if not isinstance(record, PrefixRecord):
            raise PrefixError("index requires PrefixRecord")
        if record.prefix_id in self.records:
            raise PrefixError("prefix records are append-only; id already exists")
        known = set(self.records)
        missing = [item for item in record.depends_on if item not in known]
        if missing:
            raise PrefixHistoryError("prefix dependencies missing: " + ",".join(missing))
        self.records[record.prefix_id] = record
        return record.to_dict()

    def load(self, data):
        if not isinstance(data, dict) or data.get("schema") != SCHEMA:
            raise PrefixError("unsupported prefix index schema")
        records = data.get("records")
        if not isinstance(records, list):
            raise PrefixError("prefix index records must be a list")
        self.records = {}
        for item in records:
            record = PrefixRecord.from_dict(item)
            if record.prefix_id in self.records:
                raise PrefixError("duplicate prefix id")
            self.records[record.prefix_id] = record
        # dependency closure over the complete index
        for record in self.records.values():
            missing = [item for item in record.depends_on if item not in self.records]
            if missing:
                raise PrefixHistoryError("prefix dependencies missing: " + ",".join(missing))
        for name in ("selection_log", "outcome_log"):
            log = data.get(name, [])
            if not isinstance(log, list) or any(not isinstance(item, dict) for item in log):
                raise PrefixError(name + " must be a list of objects")
            setattr(self, name, deepcopy(log))
        if self.to_dict() != data:
            raise PrefixError("prefix index bytes are not canonical")
        return self

    def select(self, *, target_id, target_revision, rmw, contract_hash,
               state_fingerprint=None, feedback_identity=None,
               include_fixtures=False, required_resources=None,
               limit=1):
        if type(limit) is not int or limit < 1:
            raise PrefixError("limit must be a positive integer")
        candidates = []
        for record in self.records.values():
            if record.target_id != target_id or record.rmw != rmw or record.contract_hash != contract_hash:
                continue
            if record.target_revision != target_revision:
                continue
            if record.fixture and not include_fixtures:
                continue
            if state_fingerprint is not None and record.state.fingerprint_id() != state_fingerprint:
                continue
            if required_resources:
                absent = sorted(set(required_resources) - set(record.resources))
                if absent:
                    continue
            candidates.append(record)
        ordered = sorted(candidates, key=lambda item: item.selection_key())
        chosen = ordered[:limit]
        event = {
            "schema": SELECTION_SCHEMA,
            "target_id": target_id, "target_revision": target_revision,
            "rmw": rmw, "contract_hash": contract_hash,
            "state_fingerprint": state_fingerprint,
            "feedback_identity": feedback_identity,
            "include_fixtures": bool(include_fixtures),
            "required_resources": sorted(required_resources or []),
            "considered": [{"prefix_id": item.prefix_id, "kind": item.kind,
                            "coverage": len(set(item.coverage)),
                            "hit_rate": item.hit_rate(), "cost": item.cost,
                            "fixture": item.fixture,
                            "selection_key": list(item.selection_key())}
                           for item in ordered],
            "chosen": [item.prefix_id for item in chosen],
            "reason": "no_eligible_prefix" if not chosen else "coverage_hitrate_cost",
            "fallback": None,
        }
        event["selection_sha256"] = canonical_hash(event)
        self.selection_log.append(event)
        if not chosen:
            raise PrefixHistoryError("no eligible raw-backed prefix; refusing random input fallback")
        return {"selection": deepcopy(event),
                "prefixes": [item.to_dict() for item in chosen]}

    def transform(self, operations, *, resources=None, require_closure=True):
        """Plan insert/replace/delete with dependency and resource guards.

        ``resources`` maps resource name to True/False/None.  Unknown resources
        never satisfy a requirement.  The method returns the resulting prefix
        ids and one guard receipt per operation; it does not execute anything.
        """
        if not isinstance(operations, list) or not operations:
            raise PrefixError("transform requires a nonempty operations list")
        if resources is not None and not isinstance(resources, dict):
            raise PrefixError("resources must be a mapping")
        resources = dict(resources or {})
        current = {identifier: record for identifier, record in self.records.items()}
        receipts, changed = [], []
        for index, operation in enumerate(operations):
            if not isinstance(operation, dict):
                raise PrefixError("transform operation must be an object")
            kind = operation.get("op")
            if kind not in {"insert", "replace", "delete"}:
                raise PrefixError("transform op must be insert/replace/delete")
            before = sorted(current)
            detail = {"op": kind, "operation_index": index, "before": before,
                      "resources": {key: resources.get(key) for key in operation.get("requires_resources", [])},
                      "status": "accepted"}
            required = operation.get("requires_resources", [])
            if (not isinstance(required, list) or any(not isinstance(item, str) or not item for item in required)):
                raise PrefixError("requires_resources must be a list of names")
            unknown = [item for item in required if resources.get(item) is None]
            denied = [item for item in required if resources.get(item) is False]
            if unknown or denied:
                detail["status"] = "blocked"
                detail["reason"] = "unknown_resource" if unknown else "resource_denied"
                detail["missing"] = sorted(unknown + denied)
                receipts.append(detail)
                raise PrefixError("prefix transform blocked: " + detail["reason"] + ":" + ",".join(detail["missing"]))
            if kind == "delete":
                identifier = operation.get("prefix_id")
                if identifier not in current:
                    raise PrefixHistoryError("delete targets unknown prefix: " + str(identifier))
                dependents = [item.prefix_id for item in current.values()
                              if identifier in item.depends_on and item.prefix_id != identifier]
                if dependents:
                    raise PrefixHistoryError("delete would break prefix dependents: " + ",".join(sorted(dependents)))
                del current[identifier]
                changed.append(identifier)
                detail["prefix_id"] = identifier
            elif kind == "insert":
                record = operation.get("record")
                if not isinstance(record, dict):
                    raise PrefixError("insert requires a record object")
                added = PrefixRecord.from_dict(record)
                if added.prefix_id in current:
                    raise PrefixError("insert prefix id already exists")
                missing = [item for item in added.depends_on if item not in current]
                if missing:
                    detail["status"] = "blocked"
                    detail["reason"] = "dependency_missing"
                    detail["missing"] = sorted(missing)
                    receipts.append(detail)
                    raise PrefixHistoryError("insert dependency missing: " + ",".join(missing))
                current[added.prefix_id] = added
                changed.append(added.prefix_id)
                detail["prefix_id"] = added.prefix_id
            else:
                identifier = operation.get("prefix_id")
                replacement = operation.get("record")
                if identifier not in current:
                    raise PrefixHistoryError("replace targets unknown prefix: " + str(identifier))
                if not isinstance(replacement, dict):
                    raise PrefixError("replace requires a record object")
                added = PrefixRecord.from_dict(replacement)
                if added.prefix_id != identifier:
                    raise PrefixError("replace must preserve the prefix id")
                dependents = [item.prefix_id for item in current.values()
                              if identifier in item.depends_on and item.prefix_id != identifier]
                current[identifier] = added
                changed.append(identifier)
                detail["prefix_id"] = identifier
                detail["dependents"] = sorted(dependents)
            after = sorted(current)
            detail["after"] = after
            receipts.append(detail)
        if require_closure:
            missing = [(record.prefix_id, item) for record in current.values()
                       for item in record.depends_on if item not in current]
            if missing:
                raise PrefixHistoryError("prefix closure broken: " + repr(sorted(missing)))
        # Commit only after every operation and the final closure check succeed.
        self.records = current
        result = {"schema": TRANSFORM_SCHEMA, "operations": receipts,
                  "prefix_ids": sorted(current), "changed": changed,
                  "closure_complete": True}
        result["transform_sha256"] = canonical_hash(result)
        return result

    def record_outcome(self, prefix_id, *, hit, coverage=None, evidence=None,
                       selection_sha256=None):
        """Attach one raw-backed outcome so later selection follows feedback.

        Counters and coverage change, but the prior record bytes stay visible in
        an ordered outcome log with before/after hashes; nothing is silently
        rewritten and no candidate is created.
        """
        if prefix_id not in self.records:
            raise PrefixHistoryError("outcome targets unknown prefix: " + str(prefix_id))
        if type(hit) is not bool:
            raise PrefixError("outcome hit must be boolean")
        if evidence is None:
            raise PrefixError("prefix outcome requires raw-backed evidence")
        _validate_receipt(evidence, 0)
        if selection_sha256 is not None:
            _sha(selection_sha256, "selection_sha256")
            if not any(item.get("selection_sha256") == selection_sha256
                       for item in self.selection_log):
                raise PrefixHistoryError("outcome must reference a recorded selection")
        current = self.records[prefix_id]
        merged = list(current.coverage)
        for item in coverage or []:
            if not isinstance(item, str) or not item:
                raise PrefixError("outcome coverage must be nonempty strings")
            if item not in merged:
                merged.append(item)
        updated = PrefixRecord(
            prefix_id=current.prefix_id, kind=current.kind, target_id=current.target_id,
            target_revision=current.target_revision, rmw=current.rmw,
            contract_hash=current.contract_hash, state=current.state,
            receipts=deepcopy(current.receipts), provenance=deepcopy(current.provenance),
            cost=current.cost, depends_on=list(current.depends_on),
            resources=list(current.resources), coverage=merged,
            attempts=current.attempts + 1, hits=current.hits + (1 if hit else 0),
            fixture=current.fixture)
        before, after = current.to_dict(), updated.to_dict()
        self.records[prefix_id] = updated
        event = {"schema": OUTCOME_SCHEMA, "sequence": len(self.outcome_log),
                 "prefix_id": prefix_id, "hit": hit,
                 "coverage_added": sorted(set(merged) - set(current.coverage)),
                 "previous_record_sha256": before["record_sha256"],
                 "record_sha256": after["record_sha256"],
                 "evidence": deepcopy(evidence),
                 "selection_sha256": selection_sha256}
        event["outcome_sha256"] = canonical_hash(event)
        self.outcome_log.append(event)
        return deepcopy(event)

    def to_dict(self):
        document = {"schema": SCHEMA,
                    "records": [self.records[key].to_dict() for key in sorted(self.records)],
                    "selection_log": deepcopy(self.selection_log),
                    "outcome_log": deepcopy(self.outcome_log)}
        document["index_sha256"] = canonical_hash(document)
        return document


def prefix_selection_provenance(selection):
    """Stable provenance fragment for runner/episode continuation."""
    if not isinstance(selection, dict) or "selection_sha256" not in selection:
        raise PrefixError("prefix selection provenance is incomplete")
    return {"schema": SELECTION_SCHEMA,
            "selection_sha256": selection["selection_sha256"],
            "chosen": list(selection["chosen"]),
            "considered": list(selection["considered"]),
            "feedback_identity": selection.get("feedback_identity")}


BINDING_SCHEMA = "r2f.s1-prefix-input-binding/v1"


def bind_prefix_input(prefix, *, final_config, target_id, target_revision, rmw,
                      contract_hash, environment=None, parameters=None,
                      mutation_kind=None, carrier=None):
    """Bind a prefix-driven variation to the existing deterministic input hash.

    Prefixes never generate inputs implicitly.  This function requires the
    concrete final config plus explicit environment/parameter variation
    receipts; an unknown variation is refused rather than defaulted, and the
    resulting digest is the same ``content_identity`` used by the corpus.
    """
    if not isinstance(prefix, PrefixRecord):
        raise PrefixError("binding requires a PrefixRecord")
    if prefix.target_id != target_id or prefix.target_revision != target_revision:
        raise PrefixHistoryError("prefix target identity differs from the bound input")
    if prefix.rmw != rmw or prefix.contract_hash != contract_hash:
        raise PrefixHistoryError("prefix rmw/contract differs from the bound input")
    if not isinstance(final_config, dict) or not final_config:
        raise PrefixError("binding requires a nonempty final config")
    receipts = {}
    for name, value in (("environment", environment), ("parameters", parameters)):
        value = {} if value is None else value
        if not isinstance(value, dict):
            raise PrefixError(f"{name} variation must be an object")
        for key, item in value.items():
            if item is None:
                raise PrefixError(f"unknown {name} variation is not a pass: {key}")
        receipts[name] = snapshot(value)
    if mutation_kind is not None and (not isinstance(mutation_kind, str) or not mutation_kind):
        raise PrefixError("mutation_kind must be a nonempty string")
    entry = {"target_id": target_id, "target_revision": target_revision, "rmw": rmw,
             "contract_hash": contract_hash, "final_config": deepcopy(final_config)}
    if mutation_kind is not None:
        entry["mutation_kind"] = mutation_kind
    if carrier is not None:
        entry["carrier"] = carrier
    document = {
        "schema": BINDING_SCHEMA,
        "prefix_id": prefix.prefix_id,
        "prefix_record_sha256": prefix.to_dict()["record_sha256"],
        "state_fingerprint_id": prefix.state.fingerprint_id(),
        "final_config": deepcopy(final_config),
        "environment": receipts["environment"],
        "parameters": receipts["parameters"],
        "mutation_kind": mutation_kind,
        "carrier": carrier,
        "input_sha256": content_identity(entry),
    }
    document["binding_sha256"] = canonical_hash(document)
    return document
