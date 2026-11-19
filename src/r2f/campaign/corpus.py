"""Pure offline exploratory corpus: retention, dedupe and representative choice.

The corpus is an index over deterministic input identities.  It is not an
execution ledger, a candidate lifecycle, or a second runner.  Initial seeds and
inputs referenced by a candidate are pinned: they are never evicted.  The
capacity limit applies to evictable representatives, so pinning can make the
total record count exceed the nominal capacity without silently deleting
required seeds or candidate evidence.
"""
from __future__ import annotations

from collections import defaultdict
from copy import deepcopy
from dataclasses import dataclass, field
import json
from pathlib import Path
import re

from r2f.core.evidence import canonical_hash
from .mutation_routes import (ROUTES_SCHEMA, canonical_route_record, has_routes,
                              merge_routes, normalize_routes, routes_enabled,
                              validate_route_entry)

CAPACITY = 256
BUCKET_LIMIT = 3
SCHEMA = "r2f.s1-corpus/v1"

PASS_STATUSES = frozenset({"pass", "fail", "unknown", "not_applicable"})
ADMISSION_REASONS = frozenset({
    "initial_seed", "forced_t0_contract", "new_contract_branch",
    "new_relation_class", "new_state_edge", "shorter_equivalent_reproduction",
    "candidate_reference",
})
REJECTION_REASONS = frozenset({
    "duplicate_content", "success_only", "no_new_information",
    "incomplete_observation", "tool_error", "not_executed",
    "bucket_representative_limit", "capacity_limit",
})

# Semantic corpus identity is distinct from the full effective replay input
# identity.  In particular, a checked generation chain is not an execution
# instruction.  All semantic hashes use an explicit new domain; old hashes
# fail validation on load (no implicit migration of checkpoints/route anchors).
CONTENT_IDENTITY_SCHEMA = "r2f.corpus-content/v2"
# Only the known topology target/program changes domain. Generic F1 roots,
# derived RNG anchors and historical goldens retain corpus-content/v2.
TOPOLOGY_CONTENT_IDENTITY_SCHEMA = "r2f.corpus-content/topology-v4"


def _topology_content_domain(entry, final):
    return (entry.get("target_id") == "discovery.endpoint_topology.fuzz_v1"
            and isinstance(final, dict) and set(final) == {"topology_program"}
            and isinstance(final["topology_program"], dict)
            and final["topology_program"].get("schema") == "r2f.s1-topology-program/v1")


def _project_topology_depth_selector(op, parameters):
    """Drop only the audited compile-time depth selection label on a copy.

    The request remains lossless elsewhere (plan, receipts, effective identity).
    Nested context.parameters is NOT handled: this compiler's numeric operator
    reads context.boundary_class directly; nested selector semantics are unproven.
    """
    if (op["application_phase"] != "compile_time"
            or op["surface_id"] != "S1.topology.program"
            or op["mutator_id"] != "I2.topology_program.v1"
            or set(op["parameters"]) != {"operator_id", "slot_id", "request", "seed", "input_layer_receipts"}):
        return
    request = parameters.get("request")
    if not isinstance(request, dict) or set(request) - {"operator_id", "slot_id", "context"}:
        return
    if (parameters.get("operator_id") not in {"queue.depth", "param.boundary"}
            or not isinstance(parameters.get("slot_id"), str)
            or re.fullmatch(r"S1\.topology\.qos\.depth#(?:0|[1-9][0-9]*)", parameters["slot_id"]) is None
            or request.get("operator_id") != parameters["operator_id"]
            or request.get("slot_id") != "S1.topology.qos.depth"):
        return
    # _resolve_wildcard_slot binds depth to a concrete #endpoint index.
    # Keep that resolved slot (and its index) in the semantic projection.
    context = request.get("context", {})
    # Unknown context/payload keys stay hashed, including nested parameters.
    if isinstance(context, dict):
        context.pop("boundary_class", None)
        if not context:
            request.pop("context", None)

def _project_topology_generation_route(op, parameters):
    """Canonicalize only the audited generation.action/next slot aliases.

    The handler appends the same recreate action for either slot, ignoring the
    wildcard selection index. Actual endpoint/generation/action order remains
    in final_config. Keep every full plan/receipt and effective input unchanged;
    unknown requests or audit obligations do not qualify for this projection.
    """
    from r2f.core.mutation_plan import ACTION_PROGRAM_LAYER_SCHEMA
    if (op["application_phase"] != "compile_time"
            or op["surface_id"] != "S1.topology.program"
            or op["mutator_id"] != "I2.topology_program.v1"
            or set(op["parameters"]) != {"operator_id", "slot_id", "request", "seed", "input_layer_receipts"}
            or parameters.get("operator_id") != "generation.action"
            or op["applicability_result"] != {"status": "applicable", "expected_disposition": "expect_pass",
                                              "execution_evidence": False, "candidate_eligible": False}):
        return
    slot = parameters.get("slot_id")
    if slot == "S1.topology.actions":
        requested_slot = slot
    elif isinstance(slot, str) and re.fullmatch(r"S1\.topology\.generation#(?:0|[1-9][0-9]*)", slot):
        requested_slot = "S1.topology.generation"
    else:
        return
    if parameters.get("request") != {"operator_id": "generation.action", "slot_id": requested_slot,
                                     "context": {"variant": "next"}}:
        return
    layers = parameters.get("input_layer_receipts")
    expected = {"schema": ACTION_PROGRAM_LAYER_SCHEMA, "input_path": "topology_program",
                "program_schema": "r2f.s1-topology-program/v1", "operator_id": "generation.action",
                "slot_id": slot, "expected_disposition": "expect_pass"}
    if not isinstance(layers, list) or layers != [expected]:
        return
    raw = op["parameters"]["input_layer_receipts"][0]
    if (raw.get("execution_evidence") is not False or raw.get("candidate_eligible") is not False
            or raw.get("case_hash_role") != "v2_envelope_dedupe_only"):
        return
    # Only these three selector labels change on the semantic projection copy.
    parameters["slot_id"] = "S1.topology.actions"
    parameters["request"]["slot_id"] = "S1.topology.actions"
    layers[0]["slot_id"] = "S1.topology.actions"


INPUT_IDENTITY_KEYS = (
    "target_id", "target_revision", "rmw", "contract_hash",
    "mutation_kind", "carrier", "mutation_parameters", "contract",
)


# These are generation-only fields emitted by the compile-time M01/M02/M03
# implementations in core.mutators, NOT a recursive payload blacklist.  In
# particular M06/M07 anchor/offset/window parameters are runtime-consumed and
# are never filtered.  Unknown parameters remain in the hash.
_CONTENT_SELECTION_PARAMETERS = {
    "M01": frozenset({"boundary_class", "sampling_kind", "wide_sampling"}),
    "M02": frozenset({"enum_size", "sampling_kind", "wide_sampling"}),
    "M03": frozenset({"sampling", "step", "sampling_kind", "wide_sampling",
                       "min", "max", "fraction", "parent", "parent_decade",
                       "multiplier", "direction", "clamped", "decade",
                       "declared_representatives", "selected"}),
}


def _content_fields(value, allowed, label):
    if not isinstance(value, dict) or set(value) - set(allowed):
        raise ValueError(f"unsupported corpus content {label} fields/schema")
    return deepcopy(value)


def _content_operation_parameters(parameters):
    """Preserve payload keys, projecting only declared generator envelopes.

    A QoS/filter/action parameter named 'seed', 'time', 'duration_sec', 'path'
    or 'history' is NOT globally transient.  The topology request schema alone
    declares its seed fields as generator controls; its resulting program is
    already bound by final_config.  Unknown parameter keys are retained.
    """
    if not isinstance(parameters, dict):
        raise ValueError("corpus operation parameters must be a dict")
    result = deepcopy(parameters)
    if "input_layer_receipts" in result:
        result["input_layer_receipts"] = _content_layers(result["input_layer_receipts"])
    # Exact topology parameter envelope from topology_input._layer().
    if set(parameters) == {"operator_id", "slot_id", "request", "seed", "input_layer_receipts"}:
        result.pop("seed")
        request = result["request"]
        _content_fields(request, {"operator_id", "slot_id", "context"}, "topology request")
        context = request.get("context", {})
        if not isinstance(context, dict):
            raise ValueError("corpus topology context must be a dict")
        for mapping in (context, context.get("parameters", {})):
            if not isinstance(mapping, dict):
                raise ValueError("corpus topology context.parameters must be a dict")
            for key in ("seed", "rng_seed", "rng_algorithm"):
                mapping.pop(key, None)
        if context.get("parameters") == {}:
            context.pop("parameters")
        if not context:
            request.pop("context", None)
    return result


def _content_operations(operations, *, topology_domain=False):
    from r2f.core.mutation_plan import MutationOperation
    if not isinstance(operations, list):
        raise ValueError("corpus operations must be a list")
    checked = [MutationOperation.from_dict(op).to_dict() for op in operations]
    ids = {op["op_id"]: i for i, op in enumerate(checked)}
    if len(ids) != len(checked):
        raise ValueError("duplicate corpus operation op_id")
    result = []
    for op in checked:
        status = deepcopy(op["applicability_result"])
        if "overridden_by" in status:
            if status["overridden_by"] not in ids:
                raise ValueError("unknown corpus overridden_by operation")
            status["overridden_by"] = ids[status["overridden_by"]]
        parameters = _content_operation_parameters(op["parameters"])
        if topology_domain:
            _project_topology_depth_selector(op, parameters)
            _project_topology_generation_route(op, parameters)
        if op["application_phase"] == "compile_time":
            for key in _CONTENT_SELECTION_PARAMETERS.get(op["mutator_id"], ()):
                parameters.pop(key, None)
        # Order is the list order, never the spelling of a generated op_id.
        result.append({"schema": op["schema"], "surface_id": op["surface_id"],
                       "mutator_id": op["mutator_id"],
                       "parameters": parameters,
                       "application_phase": op["application_phase"],
                       "applicability_result": status})
    return result


def _content_layers(layers, operations=None):
    """Project the actual provider/action-program receipt schemas.

    Action-program before/after documents and v2_case are generation proof,
    not extra submitted programs.  The execution program is final_config;
    operator/slot/disposition and request parameters remain semantic inputs.
    Unknown receipt shapes fail closed rather than lose future instructions.
    """
    from r2f.core.mutation_plan import ACTION_PROGRAM_LAYER_SCHEMA
    if not isinstance(layers, list):
        raise ValueError("corpus input_layer_receipts must be a list")
    result = []
    for layer in layers:
        if not isinstance(layer, dict):
            raise ValueError("corpus input layer must be a dict")
        if layer.get("schema") == ACTION_PROGRAM_LAYER_SCHEMA:
            fields = {"schema", "layer_index", "input_path", "program_schema", "seed",
                      "input_before", "input_after", "input_before_sha256", "input_after_sha256",
                      "operator_id", "slot_id", "expected_disposition", "case_hash_role",
                      "v2_case", "execution_evidence", "candidate_eligible"}
            _content_fields(layer, fields, "action-program layer")
            required = {"schema", "input_path", "program_schema", "input_before", "input_after"}
            if not required <= set(layer):
                raise ValueError("corpus action-program layer missing fields")
            result.append({key: deepcopy(layer[key]) for key in
                           ("schema", "input_path", "program_schema", "operator_id",
                            "slot_id", "expected_disposition") if key in layer})
        elif "plan_hash" in layer:
            fields = {"layer", "declared_mutation", "plan_hash", "content_sha256",
                      "rng_algorithm", "rng_seed", "operation_status", "runtime_windows"}
            _content_fields(layer, fields, "plan provider layer")
            if operations is None:
                raise ValueError("corpus plan provider layer requires operations")
            statuses = layer.get("operation_status")
            expected = {op["op_id"]: op["applicability_result"]["status"] for op in operations}
            if statuses != expected:
                raise ValueError("corpus provider operation_status mismatch")
            result.append({key: deepcopy(layer[key]) for key in
                           ("layer", "declared_mutation", "runtime_windows") if key in layer})
        elif "layer" in layer:
            fields = {"layer", "declared_mutation", "implementation", "parameters",
                      "submitted_reader_delay_sec"}
            result.append(_content_fields(layer, fields, "legacy provider layer"))
        else:
            raise ValueError("unsupported corpus input layer schema")
    return result


def content_identity(entry):
    """Hash final semantic input, not seed lineage or full replay provenance.

    Version policy: only the exact known topology target/program uses the
    topology-v4 domain, for roots AND derived inputs. Generic inputs retain v2.
    Existing stored hashes are never auto-migrated.
    Full effective_input_hash, MutationPlan hashes and historical artifacts are
    intentionally untouched.  All payload dictionaries are copied losslessly.
    """
    if not isinstance(entry, dict):
        raise ValueError("corpus entry must be a dict")
    identity = {key: deepcopy(entry[key]) for key in INPUT_IDENTITY_KEYS if key in entry}
    final = entry.get("final_config", entry.get("final_input"))
    if "final_config" in entry and "final_input" in entry and entry["final_config"] != entry["final_input"]:
        raise ValueError("corpus final_config/final_input mismatch")
    identity["final_config"] = deepcopy(final)
    required = ("target_id", "target_revision", "rmw", "contract_hash")
    if any(key not in identity for key in required) or final is None:
        raise ValueError("corpus content identity requires target/revision/rmw/contract/final config")
    if not isinstance(final, dict) or not final:
        raise ValueError("corpus final_config must be a nonempty dict")

    operations = entry.get("operations")
    windows = entry.get("runtime_windows")
    if "operations" in entry and not isinstance(operations, list):
        raise ValueError("corpus operations must be a list")
    if "runtime_windows" in entry and not isinstance(windows, list):
        raise ValueError("corpus runtime_windows must be a list")
    if "mutation_plan" in entry:
        from r2f.core.mutation_plan import MutationPlan
        # from_dict validates the raw lineage; do not hash that lineage.
        plan = MutationPlan.from_dict(entry["mutation_plan"])
        if plan.final_config != final or plan.contract_hash != identity["contract_hash"]:
            raise ValueError("corpus mutation_plan final_config/contract mismatch")
        plan_operations = [op.to_dict() for op in plan.operations]
        if operations is not None and operations != plan_operations:
            raise ValueError("corpus mutation_plan/operations mismatch")
        if windows is not None and windows != plan.runtime_windows:
            raise ValueError("corpus mutation_plan/runtime_windows mismatch")
        operations, windows = plan_operations, plan.runtime_windows
    if operations is not None:
        identity["operations"] = _content_operations(
            operations, topology_domain=_topology_content_domain(entry, final))
    if windows is not None:
        from r2f.core.mutation_plan import _validate_runtime_windows
        identity["runtime_windows"] = _validate_runtime_windows(windows)
    if "input_layer_receipts" in entry:
        identity["input_layer_receipts"] = _content_layers(entry["input_layer_receipts"], operations)
    schema = (TOPOLOGY_CONTENT_IDENTITY_SCHEMA if _topology_content_domain(entry, final)
              else CONTENT_IDENTITY_SCHEMA)
    return canonical_hash({"schema": schema, "identity": identity})


def content_sha256(entry):
    digest = content_identity(entry)
    if entry.get("content_sha256") not in (None, digest):
        raise ValueError("corpus content hash mismatch (semantic content identity; no implicit migration)")
    return digest


def bucket_key(entry):
    """Retention bucket: target × boundary × edge × sampling class × depth.

    Sampling class and chain depth are input-selection provenance, so two
    distinct samplers or chain depths must not evict each other merely because
    they share a boundary label.  Missing values stay ``""`` so pre-T-E records
    keep their historical bucket grouping (and bucket_limit/capacity semantics).
    """
    edge = entry.get("observed_edge") or ""
    kind = entry.get("sampling_kind") or ""
    depth = entry.get("chain_depth")
    if type(depth) is not int:
        depth = ""
    return (entry.get("target_id"), entry.get("boundary_class") or "", edge,
            kind, depth)


def semantic_key(entry):
    return (entry.get("target_id"), entry.get("surface_id"), entry.get("boundary_class"),
            entry.get("relation_class"), entry.get("observed_edge"))


def _rank(entry):
    """Prefer complete evidence, then shorter/cheaper, then stable content id."""
    cost = entry.get("cost", 0)
    if isinstance(cost, bool) or not isinstance(cost, int):
        cost = 0
    return (entry.get("evidence_complete") is not True, cost, content_sha256(entry))


def _normal_record(entry, *, seed=False, candidate=False, source="observation"):
    record = canonical_route_record(entry) if has_routes(entry) else deepcopy(entry)
    record["content_sha256"] = content_sha256(entry)
    if has_routes(record) and record["content_sha256"] != content_identity(record):
        raise ValueError("routed corpus content hash mismatch")
    record["pinned_seed"] = bool(seed or record.get("pinned_seed"))
    record["candidate_ids"] = sorted(set(record.get("candidate_ids") or []))
    record["source"] = source
    record["bucket_key"] = list(bucket_key(record))
    return record


@dataclass
class Corpus:
    capacity: int = CAPACITY
    bucket_limit: int = BUCKET_LIMIT
    entries: dict = field(default_factory=dict)       # content sha -> record
    buckets: dict = field(default_factory=lambda: defaultdict(set))
    semantics: dict = field(default_factory=lambda: defaultdict(set))
    candidates: dict = field(default_factory=lambda: defaultdict(set))
    history: list = field(default_factory=list)
    source_ingestion_receipt: dict | None = None  # optional topology provenance; not input identity

    def __post_init__(self):
        if type(self.capacity) is not int or self.capacity < 1:
            raise ValueError("corpus capacity must be a positive integer")
        if type(self.bucket_limit) is not int or self.bucket_limit < 1:
            raise ValueError("corpus bucket_limit must be a positive integer")

    def _log(self, action, content, reason, **details):
        self.history.append({"sequence": len(self.history), "action": action,
                             "content_sha256": content, "reason": reason, **details})

    def _is_pinned(self, record):
        return record.get("pinned_seed") is True or bool(record.get("candidate_ids"))

    def _evictable(self):
        return [record for record in self.entries.values() if not self._is_pinned(record)]

    def _rebuild_indexes(self):
        self.buckets = defaultdict(set)
        self.semantics = defaultdict(set)
        for digest, record in self.entries.items():
            self.buckets[bucket_key(record)].add(digest)
            self.semantics[semantic_key(record)].add(digest)

    def _evict(self, digest, reason, *, bucket=None):
        record = self.entries.pop(digest)
        bucket = bucket if bucket is not None else bucket_key(record)
        if bucket in self.buckets:
            self.buckets[bucket].discard(digest)
            if not self.buckets[bucket]:
                self.buckets.pop(bucket, None)
        self.semantics[semantic_key(record)].discard(digest)
        self._log("evicted", digest, reason, seed_id=record.get("seed_id"),
                  bucket=list(bucket_key(record)))
        return record

    def _trim_bucket(self, key):
        digests = sorted((d for d in self.buckets.get(key, set())
                          if not self._is_pinned(self.entries[d])),
                         key=lambda d: _rank(self.entries[d]))
        for digest in digests[self.bucket_limit:]:
            self._evict(digest, "bucket_representative_limit", bucket=key)

    def _trim_capacity(self):
        while len(self._evictable()) > self.capacity:
            victim = sorted(self._evictable(), key=_rank)[-1]
            self._evict(victim["content_sha256"], "capacity_limit")

    def add_seed(self, entry):
        """Register a fixed initial seed; seeds are always retained."""
        record = _normal_record(entry, seed=True, source="initial_seed")
        digest = record["content_sha256"]
        if digest in self.entries:
            merge_routes(self.entries[digest], record)
            self.entries[digest]["bucket_key"] = list(bucket_key(self.entries[digest]))
            self._rebuild_indexes()
            self.entries[digest]["pinned_seed"] = True
            self._log("retained", digest, "initial_seed", seed_id=record.get("seed_id"))
            return self.entries[digest]
        record["admission_reason"] = "initial_seed"
        self.entries[digest] = record
        self._rebuild_indexes()
        self._trim_bucket(bucket_key(record))
        self._trim_capacity()
        self._log("admitted", digest, "initial_seed", seed_id=record.get("seed_id"))
        return deepcopy(record)

    def pin_candidate(self, content, candidate_id):
        """Pin an existing input referenced by a candidate, without adding a hit."""
        if not isinstance(candidate_id, str) or not candidate_id.strip():
            raise ValueError("candidate_id must be a nonempty string")
        digest = content if isinstance(content, str) else content_sha256(content)
        record = self.entries.get(digest)
        if record is None:
            if not isinstance(content, dict):
                raise ValueError("pinning an unseen candidate requires its input record")
            record = _normal_record(content, candidate=True, source="candidate_reference")
            record["admission_reason"] = "candidate_reference"
            self.entries[digest] = record
        if isinstance(content, dict) and has_routes(content):
            merge_routes(record, _normal_record(content))
            record["bucket_key"] = list(bucket_key(record))
        record.setdefault("candidate_ids", [])
        if candidate_id not in record["candidate_ids"]:
            record["candidate_ids"].append(candidate_id)
            record["candidate_ids"].sort()
        self.candidates[candidate_id].add(digest)
        self._rebuild_indexes()
        self._trim_capacity()
        self._log("pinned", digest, "candidate_reference", candidate_id=candidate_id)
        return deepcopy(record)

    def _novelty(self, record, *, evidence_complete, observed_edge):
        if record.get("contract_branch") and record["contract_branch"] not in {
                r.get("contract_branch") for r in self.entries.values()}:
            return "new_contract_branch"
        relation = (record.get("target_id"), record.get("surface_id"),
                    record.get("boundary_class"), record.get("relation_class"))
        if record.get("relation_class") and not any(
                (r.get("target_id"), r.get("surface_id"), r.get("boundary_class"), r.get("relation_class")) == relation
                for r in self.entries.values()):
            return "new_relation_class"
        edge = (record.get("target_id"), record.get("boundary_class"), observed_edge)
        if observed_edge and not any(
                (r.get("target_id"), r.get("boundary_class"), r.get("observed_edge")) == edge
                for r in self.entries.values()):
            return "new_state_edge"
        if evidence_complete is True:
            for existing in self.entries.values():
                if semantic_key(existing) == semantic_key(record) and _rank(record)[1] < _rank(existing)[1]:
                    return "shorter_equivalent_reproduction"
        return None

    def offer(self, entry, *, executed=False, evidence_complete=False,
              observed_edge=None, oracle_verdict=None, tool_error=False,
              candidate_id=None):
        """Offer an executed observation for retention.

        A successful run alone is not an admission reason.  Only contract,
        relation, state-edge or shorter-equivalent novelty is retained.
        """
        record = _normal_record(entry, source="observation")
        if observed_edge:
            record["observed_edge"] = observed_edge
            record["bucket_key"] = list(bucket_key(record))
        digest = record["content_sha256"]
        if not executed:
            self._log("rejected", digest, "not_executed")
            return {"admitted": False, "reason": "not_executed", "content_sha256": digest}
        if tool_error:
            self._log("rejected", digest, "tool_error")
            return {"admitted": False, "reason": "tool_error", "content_sha256": digest}
        # V34: incomplete observation never refreshes coverage.  This is
        # verdict-independent: a fail with missing identity/window evidence is
        # diagnostic history, not a newly covered corpus entry.
        if evidence_complete is not True:
            self._log("rejected", digest, "incomplete_observation")
            return {"admitted": False, "reason": "incomplete_observation", "content_sha256": digest}
        if digest in self.entries:
            merge_routes(self.entries[digest], record)
            self.entries[digest]["bucket_key"] = list(bucket_key(self.entries[digest]))
            self._rebuild_indexes()
            if candidate_id:
                self.pin_candidate(digest, candidate_id)
            self._log("deduplicated", digest, "duplicate_content")
            return {"admitted": False, "reason": "duplicate_content", "content_sha256": digest,
                    "existing": True}
        reason = "candidate_reference" if candidate_id else self._novelty(
            record, evidence_complete=evidence_complete, observed_edge=observed_edge)
        if reason is None:
            reason = "success_only" if oracle_verdict == "pass" else "no_new_information"
            self._log("rejected", digest, reason)
            return {"admitted": False, "reason": reason, "content_sha256": digest}
        record["admission_reason"] = reason
        record["evidence_complete"] = bool(evidence_complete)
        record["oracle_verdict"] = oracle_verdict
        self.entries[digest] = record
        self._rebuild_indexes()
        if candidate_id:
            self.pin_candidate(digest, candidate_id)
        self._trim_bucket(bucket_key(record))
        self._trim_capacity()
        self._log("admitted", digest, reason, seed_id=record.get("seed_id"),
                  bucket=list(bucket_key(record)))
        return {"admitted": True, "reason": reason, "content_sha256": digest}

    def selectable(self):
        return sorted((deepcopy(record) for record in self.entries.values()
                       if record.get("executable", True) is not False),
                      key=lambda r: (r.get("target_id", ""), r.get("seed_id", ""), r["content_sha256"]))

    @classmethod
    def from_dict(cls, data, *, model_parents=None):
        """Restore an index, never re-admit observations as newly pinned seeds.

        Input identities are recomputed, but the original records/history are
        copied verbatim.  Evicted parents remain valid historical references.
        A caller may supply hash->input_record model parents ONLY after its
        frontier.validate_model(...) succeeds.  We independently check their
        identities, ancestry and edge bindings; they are not novelty evidence
        and are never inserted into entries/history or serialized on return.
        The default (including load()) still rejects missing parent inputs.
        """
        if not isinstance(data, dict) or data.get("schema") != SCHEMA:
            raise ValueError("unsupported corpus schema")
        corpus = cls(capacity=data["capacity"], bucket_limit=data["bucket_limit"])
        records, history = data.get("entries"), data.get("history")
        if not isinstance(records, list) or not isinstance(history, list):
            raise ValueError("corpus entries/history must be lists")
        for record in records:
            if not isinstance(record, dict):
                raise ValueError("corpus record must be a dict")
            digest = record.get("content_sha256")
            if not isinstance(digest, str) or not re.fullmatch(r"[0-9a-f]{64}", digest):
                raise ValueError("invalid corpus content hash")
            if has_routes(record) and canonical_route_record(record) != record:
                raise ValueError("noncanonical corpus mutation routes/labels")
            if digest != content_identity(record):
                raise ValueError("corpus content hash mismatch")
            if digest in corpus.entries:
                raise ValueError("duplicate corpus content")
            if type(record.get("pinned_seed")) is not bool:
                raise ValueError("invalid corpus pinned flag")
            candidates = record.get("candidate_ids")
            if (not isinstance(candidates, list) or
                    any(not isinstance(c, str) or not c.strip() for c in candidates) or
                    candidates != sorted(set(candidates))):
                raise ValueError("invalid corpus candidate references")
            if record.get("bucket_key") != list(bucket_key(record)):
                raise ValueError("corpus bucket index mismatch")
            parent = record.get("parent_content_sha256")
            if parent is not None and (not isinstance(parent, str) or
                    not re.fullmatch(r"[0-9a-f]{64}", parent) or parent == digest):
                raise ValueError("invalid corpus parent reference")
            corpus.entries[digest] = deepcopy(record)
            for candidate in candidates:
                corpus.candidates[candidate].add(digest)
        for sequence, event in enumerate(history):
            if (not isinstance(event, dict) or type(event.get("sequence")) is not int or
                    event["sequence"] != sequence or
                    event.get("action") not in {"admitted", "retained", "pinned", "rejected", "evicted", "deduplicated"} or
                    not isinstance(event.get("content_sha256"), str) or
                    not re.fullmatch(r"[0-9a-f]{64}", event["content_sha256"]) or
                    not isinstance(event.get("reason"), str)):
                raise ValueError("invalid corpus history order/event")
        corpus.history = deepcopy(history)
        external = {}
        if model_parents is not None:
            from collections.abc import Mapping
            if not isinstance(model_parents, Mapping):
                raise ValueError("corpus model_parents must be a mapping")
            for digest, record in model_parents.items():
                if (not isinstance(digest, str) or not re.fullmatch(r"[0-9a-f]{64}", digest)
                        or not isinstance(record, dict) or content_sha256(record) != digest):
                    raise ValueError("corpus model parent content hash mismatch")
                parent = record.get("parent_content_sha256")
                if parent is not None and (not isinstance(parent, str) or
                        not re.fullmatch(r"[0-9a-f]{64}", parent) or parent == digest):
                    raise ValueError("invalid corpus model parent reference")
                external[digest] = deepcopy(record)
        # Prefer the corpus's own retained lineage for shared semantic content;
        # model generation metadata must not replace the original corpus audit.
        ancestry = {**external, **corpus.entries}
        known = set(ancestry) | {e["content_sha256"] for e in history}
        binding = ("target_id", "target_revision", "rmw", "contract_hash")
        for digest, record in [*corpus.entries.items(), *external.items()]:
            parent = record.get("parent_content_sha256")
            if parent is not None and parent not in known:
                raise ValueError("corpus parent is missing from retained/historical inputs")
            child, child_digest = record, digest
            seen = {digest}
            while parent in ancestry:
                if parent in seen:
                    raise ValueError("cyclic corpus parent lineage")
                seen.add(parent)
                parent_record = ancestry[parent]
                if external and (parent in external or child_digest in external):
                    if any(parent_record[key] != child[key] for key in binding):
                        raise ValueError("corpus model parent target/revision/RMW/contract mismatch")
                child, child_digest = parent_record, parent
                parent = parent_record.get("parent_content_sha256")
        corpus._rebuild_indexes()
        if len(corpus._evictable()) > corpus.capacity:
            raise ValueError("corpus exceeds capacity")
        if any(sum(not corpus._is_pinned(corpus.entries[d]) for d in members) > corpus.bucket_limit
               for members in corpus.buckets.values()):
            raise ValueError("corpus exceeds bucket limit")
        if any(r.get('source_schema') == 'r2f.s1-seed-corpus/v2' for r in records) and \
                'source_ingestion_receipt' not in data:
            raise ValueError('topology_source_ingestion_receipt_missing')
        if 'source_ingestion_receipt' in data:
            corpus.source_ingestion_receipt = deepcopy(data['source_ingestion_receipt'])
            _validate_topology_ingestion(corpus.source_ingestion_receipt, corpus.entries.values())
        # Includes all serialized counts and explicit indexes.  Never repair a
        # damaged checkpoint by dropping records or silently rebuilding claims.
        rebuilt = corpus.to_dict()
        if data != rebuilt:
            raise ValueError("corpus indexes/counts or fields do not match records")
        return corpus

    @classmethod
    def load(cls, path):
        return cls.from_dict(json.loads(Path(path).read_text()))

    def _index_dict(self):
        def index(records, key):
            result = defaultdict(list)
            for digest, record in records.items():
                result[key(record)].append(digest)
            return [{"key": list(k), "members": sorted(v)}
                    for k, v in sorted(result.items(), key=lambda item: canonical_hash(list(item[0])))]
        return {"buckets": index(self.entries, bucket_key),
                "semantics": index(self.entries, semantic_key),
                "candidates": {k: sorted(v) for k, v in sorted(self.candidates.items())}}

    def to_dict(self):
        result = {"schema": SCHEMA, "capacity": self.capacity, "bucket_limit": self.bucket_limit,
                "entry_count": len(self.entries), "evictable_count": len(self._evictable()),
                "pinned_seed_count": sum(r.get("pinned_seed") is True for r in self.entries.values()),
                "candidate_reference_count": len(self.candidates),
                "capacity_controlled": len(self._evictable()) <= self.capacity,
                "entries": sorted((deepcopy(r) for r in self.entries.values()),
                                  key=lambda r: r["content_sha256"]),
                "history": deepcopy(self.history), "indexes": self._index_dict()}
        if self.source_ingestion_receipt is not None:
            result['source_ingestion_receipt'] = deepcopy(self.source_ingestion_receipt)
        return result


def corpus_identity(corpus):
    """Index identity; excludes history order and execution metadata."""
    if isinstance(corpus, Corpus):
        corpus = corpus.to_dict()
    members = sorted(({key: row.get(key) for key in
                       ("content_sha256", "target_id", "seed_id", "pinned_seed", "candidate_ids")}
                      for row in corpus.get("entries", [])),
                     key=lambda r: r["content_sha256"])
    by_content = {row["content_sha256"]: row for row in corpus.get("entries", [])}
    for member in members:
        record = by_content[member["content_sha256"]]
        if has_routes(record):
            member.update(mutation_routes_schema=record["mutation_routes_schema"],
                          mutation_routes=deepcopy(record["mutation_routes"]),
                          mutation_route_origins=deepcopy(record["mutation_route_origins"]))
    return canonical_hash({"schema": SCHEMA, "capacity": corpus.get("capacity"),
                           "bucket_limit": corpus.get("bucket_limit"), "members": members})


# ---------------------------------------------------------------------------
# Profile -> corpus construction (T02/T05, pure offline).
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# Profile -> corpus construction (T02/T05, pure offline).
# ---------------------------------------------------------------------------

class CorpusProfileError(ValueError):
    """A profile cannot be turned into an honest live seed corpus."""


_PROFILE_SCHEMA = "r2f.s1-fuzz-profile/v1"
_RUNNER_SLICE_CURRENT = "endpoint_pair_slice"


def _runner_cycle_targets(profile=None):
    """Targets the shared exploratory runner can currently turn into cases.

    Imported lazily to avoid an import cycle (the runner imports this module).
    The set is the runner's own gate; this module never widens it, it only
    refuses to build a corpus the runner would reject case by case.
    """
    from r2f.campaign.runner import _F1_CYCLE_TARGETS, _F2_CYCLE_TARGETS
    from r2f.campaign.exploratory_input import validate_f2_profile
    enabled = validate_f2_profile(profile or {})
    return frozenset(_F1_CYCLE_TARGETS) | (_F2_CYCLE_TARGETS if enabled else frozenset())


def _load_profile_document(profile):
    if isinstance(profile, (str, bytes)) or hasattr(profile, "__fspath__"):
        import json
        from pathlib import Path
        profile = json.loads(Path(profile).read_text())
    if not isinstance(profile, dict):
        raise CorpusProfileError("profile must be an object or a JSON path")
    return deepcopy(profile)


def _profile_strings(profile, key):
    value = profile.get(key)
    if not isinstance(value, list) or not value:
        raise CorpusProfileError(f"profile {key} must be a nonempty list")
    if any(not isinstance(item, str) or not item for item in value):
        raise CorpusProfileError(f"profile {key} must contain nonempty strings")
    if len(set(value)) != len(value):
        raise CorpusProfileError(f"profile {key} contains duplicates")
    return value


def _profile_seed_specs(specs, *, context="profile", routed=False):
    if not isinstance(specs, list) or not specs:
        raise CorpusProfileError(f"{context} seed_specs must be a nonempty list")
    seen = set()
    normalized = []
    for index, spec in enumerate(specs):
        if not isinstance(spec, dict):
            raise CorpusProfileError(f"{context} seed_specs[{index}] must be an object")
        seed_id = spec.get("seed_id")
        target_id = spec.get("target_id")
        boundary = spec.get("boundary_class")
        variant = spec.get("variant_id") or seed_id
        executable = spec.get("executable")
        if not isinstance(seed_id, str) or not seed_id:
            raise CorpusProfileError(f"{context} seed_specs[{index}].seed_id is required")
        if not isinstance(target_id, str) or not target_id:
            raise CorpusProfileError(f"{context} seed_specs[{index}].target_id is required")
        if not isinstance(boundary, str) or not boundary:
            raise CorpusProfileError(f"{context} seed_specs[{index}].boundary_class is required")
        if executable is not True:
            raise CorpusProfileError(
                f"{context} seed_specs[{index}] {seed_id}: live corpus entries must be executable=true")
        key = (seed_id, target_id, variant)
        if key in seen:
            raise CorpusProfileError(
                f"{context} seed_specs duplicates {seed_id}/{variant} for {target_id}")
        seen.add(key)
        overrides = spec.get("overrides", {})
        if not isinstance(overrides, dict):
            raise CorpusProfileError(f"{context} seed_specs[{index}].overrides must be an object")
        logical = overrides.get("logical_config", {})
        if not isinstance(logical, dict):
            raise CorpusProfileError(
                f"{context} seed_specs[{index}].overrides.logical_config must be an object")
        normalized.append({"seed_id": seed_id, "target_id": target_id,
                           "variant_id": variant,
                           "boundary_class": boundary, "executable": True,
                           "overrides": {"logical_config": deepcopy(logical)}})
        if routed:
            try:
                normalized[-1]["mutation_routes"] = normalize_routes(spec.get("mutation_routes"))
            except ValueError as exc:
                raise CorpusProfileError(str(exc)) from exc
        elif "mutation_routes" in spec or "mutation_routes_schema" in spec:
            raise CorpusProfileError("mutation_routes require explicit profile opt-in")
        if "mutation_routes_schema" in spec:
            raise CorpusProfileError("route schema belongs to profile composition, not seed spec")
    return normalized


def _profile_cycles(profile):
    """Resolve explicit per-target cycles.

    A single profile may declare several targets, but each cycle is one target
    plus its own seed list.  Building a corpus requires choosing exactly one
    cycle, so a mixed corpus can never be handed to an endpoint-pair runner.
    """
    try:
        routed = routes_enabled(profile)
    except ValueError as exc:
        raise CorpusProfileError(str(exc)) from exc
    cycles = profile.get("cycles")
    if cycles is None:
        # Transitional shape: one target, seed specs directly on the profile.
        targets = _profile_strings(profile, "target_ids")
        if len(targets) != 1:
            raise CorpusProfileError(
                "profile with multiple target_ids must declare explicit cycles")
        cycles = [{
            "cycle_id": _profile_strings(profile, "surfaces")[0],
            "target_id": targets[0],
            "surfaces": _profile_strings(profile, "surfaces"),
            "seed_specs": profile.get("seed_specs"),
            "runner_slice": profile.get("runner_slice", _RUNNER_SLICE_CURRENT),
        }]
    if not isinstance(cycles, list) or not cycles:
        raise CorpusProfileError("profile cycles must be a nonempty list")
    normalized = []
    seen = set()
    for index, cycle in enumerate(cycles):
        if not isinstance(cycle, dict):
            raise CorpusProfileError(f"profile cycles[{index}] must be an object")
        cycle_id = cycle.get("cycle_id")
        if not isinstance(cycle_id, str) or not cycle_id:
            raise CorpusProfileError(f"profile cycles[{index}].cycle_id is required")
        if cycle_id in seen:
            raise CorpusProfileError(f"profile cycles duplicates cycle_id {cycle_id}")
        seen.add(cycle_id)
        target_id = cycle.get("target_id")
        if not isinstance(target_id, str) or not target_id:
            raise CorpusProfileError(f"profile cycle {cycle_id}.target_id is required")
        surfaces = cycle.get("surfaces")
        if not isinstance(surfaces, list) or not surfaces or any(
                not isinstance(item, str) or not item for item in surfaces):
            raise CorpusProfileError(f"profile cycle {cycle_id}.surfaces must be nonempty ids")
        if len(set(surfaces)) != len(surfaces):
            raise CorpusProfileError(f"profile cycle {cycle_id}.surfaces contains duplicates")
        specs = _profile_seed_specs(cycle.get("seed_specs"),
                                   context=f"profile cycle {cycle_id}", routed=routed)
        if any(spec["target_id"] != target_id for spec in specs):
            raise CorpusProfileError(
                f"profile cycle {cycle_id} contains a seed for a different target")
        runner_slice = cycle.get("runner_slice", _RUNNER_SLICE_CURRENT)
        if not isinstance(runner_slice, str) or not runner_slice:
            raise CorpusProfileError(f"profile cycle {cycle_id}.runner_slice is required")
        normalized.append({"cycle_id": cycle_id, "target_id": target_id,
                           "surfaces": list(surfaces), "seed_specs": specs,
                           "runner_slice": runner_slice})
    return normalized


def available_cycles(profile):
    """Offline view of a profile's cycles and their runner availability."""
    profile = _load_profile_document(profile)
    supported = _runner_cycle_targets(profile)
    return [{"cycle_id": cycle["cycle_id"], "target_id": cycle["target_id"],
             "surfaces": list(cycle["surfaces"]),
             "seed_specs": [dict(spec) for spec in cycle["seed_specs"]],
             "seed_ids": [spec["seed_id"] for spec in cycle["seed_specs"]],
             "runner_slice": cycle["runner_slice"],
             "buildable": (cycle["runner_slice"] == _RUNNER_SLICE_CURRENT
                           and cycle["target_id"] in supported),
             "reason": None if (cycle["runner_slice"] == _RUNNER_SLICE_CURRENT
                                and cycle["target_id"] in supported)
             else f"unsupported_capability:{cycle['runner_slice']}"}
            for cycle in _profile_cycles(profile)]


def corpus_from_profile(profile, seeds=None, *, cycle_id=None, target_id=None):
    """Build one deterministic live seed Corpus for exactly one profile cycle.

    Pure offline: validates profile/catalog/seed references and registers
    initial templates.  It never calls a runtime, opens a socket, starts ROS,
    or writes execution evidence.

    A profile may declare several targets, but this function never returns a
    mixed corpus.  ``cycle_id`` (preferred) or ``target_id`` must select one
    declared cycle.  A cycle whose target is outside the shared runner's
    current endpoint-pair slice is refused with an explicit capability reason
    instead of producing a corpus that the runner would reject case by case.

    F1 remains the default. Only the explicit f2_windows_v1 profile/new
    target may authorize new E02/M07 or E03/M06 entries from pending templates;
    Exact G01 target selection separately authorizes its revision1 M08 binding.
    Neither path edits authored generation-template provenance or widens F1 surfaces.
    """
    from r2f.modules.s1 import fuzz_catalog
    from r2f.modules.s1.targets import (
        EXPLORATORY_RUNTIME_TARGET, F2_WINDOWS_RUNTIME_TARGET,
        exploratory_target_declarations,
    )
    from r2f.core.mutators import MUTATORS
    from r2f.campaign.exploratory_input import (
        F2_SURFACE_MUTATORS, F2_WINDOWS_CAPABILITY, apply_profile_controls,
        validate_profile_fields, validate_f2_profile, is_g01_profile,
    )

    profile = _load_profile_document(profile)
    from r2f.modules.s1.topology_targets import TOPOLOGY_CAPABILITY, TOPOLOGY_TARGET
    if profile.get('runtime_capability') == TOPOLOGY_CAPABILITY or (
            isinstance(profile.get('target_ids'), list) and TOPOLOGY_TARGET in profile['target_ids']):
        if target_id not in (None, TOPOLOGY_TARGET):
            raise CorpusProfileError('topology_selected_target_mismatch')
        return topology_corpus_from_profile(profile, seeds, cycle_id=cycle_id)
    try:
        validate_profile_fields(profile)
        f2_windows = validate_f2_profile(profile)
        g01 = is_g01_profile(profile)
    except ValueError as exc:
        raise CorpusProfileError(str(exc)) from exc
    if profile.get("schema_version") != _PROFILE_SCHEMA:
        raise CorpusProfileError("unsupported profile schema_version")
    if profile.get("campaign_kind") != "exploratory":
        raise CorpusProfileError("profile campaign_kind must be exploratory")
    if profile.get("required_catalog_included") is not False:
        raise CorpusProfileError("exploratory profile must not include the required catalog")
    if not isinstance(profile.get("profile_id"), str) or not profile["profile_id"]:
        raise CorpusProfileError("profile profile_id is required")
    if profile.get("seed_catalog_status") != "authored_t02":
        raise CorpusProfileError("profile seed_catalog_status must be authored_t02")
    if profile.get("enabled") is not False and not (g01 and profile.get("enabled") is True):
        raise CorpusProfileError("profile must stay enabled=false; enabling is a separate owner decision")

    all_targets = _profile_strings(profile, "target_ids")
    all_surfaces = _profile_strings(profile, "surfaces")
    rmws = _profile_strings(profile, "rmws")
    cycles = _profile_cycles(profile)
    routed = routes_enabled(profile)

    declared = exploratory_target_declarations(runtime_capability=profile.get("runtime_capability"))
    for target in all_targets:
        if target not in declared:
            raise CorpusProfileError(f"profile target_ids contains unknown target {target}")
    for cycle in cycles:
        if cycle["target_id"] not in all_targets:
            raise CorpusProfileError(
                f"profile cycle {cycle['cycle_id']} target {cycle['target_id']} "
                f"is not in profile target_ids")
        for surface_id in cycle["surfaces"]:
            if surface_id not in all_surfaces:
                raise CorpusProfileError(
                    f"profile cycle {cycle['cycle_id']} surface {surface_id} "
                    f"is not in profile surfaces")

    if routed:
        for item in cycles:
            target = declared[item["target_id"]]
            for spec in item["seed_specs"]:
                for rmw in rmws:
                    try:
                        validate_route_entry({"target_id": target.target_id,
                            "target_revision": target.target_revision,
                            "contract_hash": target.exploration_contract.contract_hash(),
                            "rmw": rmw, "cycle_id": item["cycle_id"], "seed_id": spec["seed_id"],
                            "mutation_routes_schema": ROUTES_SCHEMA,
                            "mutation_routes": spec["mutation_routes"]}, profile=profile)
                    except ValueError as exc:
                        raise CorpusProfileError(str(exc)) from exc

    for surface_id in all_surfaces:
        if surface_id not in fuzz_catalog.SURFACES:
            raise CorpusProfileError(f"profile surfaces contains unknown surface {surface_id}")
        if f2_windows:
            if surface_id not in F2_SURFACE_MUTATORS or not fuzz_catalog.f2_live_surface(surface_id).executable:
                raise CorpusProfileError(f"profile surface {surface_id} has no explicit F2 live binding")
            continue
        if g01 and surface_id == "G01":
            continue
        if surface_id in fuzz_catalog.F2_SURFACE_IDS:
            raise CorpusProfileError(
                f"profile surface {surface_id} is capability pending and cannot enter the F1 live corpus")
        if surface_id not in fuzz_catalog.F1_SURFACE_IDS:
            raise CorpusProfileError(f"profile surface {surface_id} is not F1-routable")
        if not fuzz_catalog.SURFACES[surface_id].executable:
            raise CorpusProfileError(f"profile surface {surface_id} has no executable runtime binding")

    if cycle_id is None and target_id is None:
        raise CorpusProfileError(
            "explicit cycle_id or target_id is required; refusing to build a mixed corpus")
    if cycle_id is not None:
        if not isinstance(cycle_id, str) or not cycle_id:
            raise CorpusProfileError("cycle_id must be a nonempty string")
        matches = [cycle for cycle in cycles if cycle["cycle_id"] == cycle_id]
    else:
        if not isinstance(target_id, str) or not target_id:
            raise CorpusProfileError("target_id must be a nonempty string")
        matches = [cycle for cycle in cycles if cycle["target_id"] == target_id]
    if not matches:
        selector = cycle_id if cycle_id is not None else target_id
        raise CorpusProfileError(f"profile has no cycle matching {selector!r}")
    if len(matches) != 1:
        ids = ",".join(cycle["cycle_id"] for cycle in matches)
        raise CorpusProfileError(
            f"selector matches multiple cycles ({ids}); choose an explicit cycle_id")
    cycle = matches[0]
    if cycle_id is not None and target_id is not None and cycle["target_id"] != target_id:
        raise CorpusProfileError(
            f"cycle {cycle_id} belongs to {cycle['target_id']}, not {target_id}")

    supported = _runner_cycle_targets(profile)
    if cycle["runner_slice"] != _RUNNER_SLICE_CURRENT:
        raise CorpusProfileError(
            f"cycle {cycle['cycle_id']} ({cycle['target_id']}) is "
            f"{cycle['runner_slice']}; the shared runner slice does not support it")
    if cycle["target_id"] not in supported:
        raise CorpusProfileError(
            f"cycle {cycle['cycle_id']} target {cycle['target_id']} is not in the "
            f"shared runner endpoint-pair slice; build refused rather than emitting "
            f"entries the runner would reject")

    surfaces = list(cycle["surfaces"])
    specs = cycle["seed_specs"]

    declared_target = declared.get(cycle["target_id"])
    if declared_target is None:
        raise CorpusProfileError(f"cycle target {cycle['target_id']} is not declared")
    cycle_contract = declared_target.exploration_contract
    if cycle_contract is None:
        raise CorpusProfileError(f"target {cycle['target_id']} has no exploration contract")
    # Every declared surface must actually belong to this target, even if it
    # has no seed yet.  Otherwise the profile would claim coverage the target
    # contract cannot express.
    for surface_id in surfaces:
        if surface_id not in cycle_contract.surface_by_id:
            raise CorpusProfileError(
                f"target {cycle['target_id']} does not declare surface {surface_id}")

    pending_seeds = profile.get("pending_seed_ids", [])
    pending_surfaces = profile.get("pending_surfaces", [])
    if not isinstance(pending_seeds, list) or any(not isinstance(s, str) or not s for s in pending_seeds):
        raise CorpusProfileError("profile pending_seed_ids must be a list of ids")
    if not isinstance(pending_surfaces, list) or any(not isinstance(s, str) or not s for s in pending_surfaces):
        raise CorpusProfileError("profile pending_surfaces must be a list of ids")
    for surface_id in pending_surfaces:
        if surface_id not in fuzz_catalog.SURFACES:
            raise CorpusProfileError(f"profile pending_surfaces contains unknown surface {surface_id}")
        if surface_id in surfaces:
            raise CorpusProfileError(
                f"surface {surface_id} is both live in the selected cycle and pending")
    for surface_id in surfaces:
        if surface_id in pending_surfaces:
            raise CorpusProfileError(f"surface {surface_id} cannot be both live and pending")

    if seeds is None:
        loaded = fuzz_catalog.load_seeds()
        seed_map = loaded["seeds"]
        if loaded["pending"]:
            raise CorpusProfileError(
                "seed catalog still contains pending entries: " + ",".join(sorted(loaded["pending"])))
    else:
        if isinstance(seeds, dict) and "seeds" in seeds and isinstance(seeds["seeds"], dict):
            seeds = seeds["seeds"]
        if not isinstance(seeds, dict):
            raise CorpusProfileError("seeds must be a mapping of seed_id to seed object")
        seed_map = seeds

    for seed_id in pending_seeds:
        if seed_id not in seed_map:
            raise CorpusProfileError(f"profile pending_seed_ids contains unknown seed {seed_id}")
        seed = seed_map[seed_id]
        if seed.get("capability", {}).get("f1_routable") is not False:
            raise CorpusProfileError(
                f"pending seed {seed_id} is actually F1-routable; pending metadata is inconsistent")
        if seed_id in {spec["seed_id"] for spec in specs}:
            raise CorpusProfileError(f"seed {seed_id} is both live and pending")

    corpus_config = profile.get("corpus", {})
    if not isinstance(corpus_config, dict):
        raise CorpusProfileError("profile corpus must be an object")
    capacity = corpus_config.get("capacity", CAPACITY)
    bucket_limit = corpus_config.get("bucket_limit", BUCKET_LIMIT)
    try:
        corpus = Corpus(capacity=capacity, bucket_limit=bucket_limit)
    except ValueError as exc:
        raise CorpusProfileError("invalid corpus limits: " + str(exc)) from exc

    live_seed_variants = set()
    for spec in specs:
        seed_id = spec["seed_id"]
        variant_id = spec.get("variant_id", seed_id)
        if variant_id in live_seed_variants:
            raise CorpusProfileError(f"seed variant {variant_id} is declared twice in the live corpus")
        live_seed_variants.add(variant_id)
        if seed_id in pending_seeds:
            raise CorpusProfileError(f"seed {seed_id} is both live and pending")
        if seed_id not in seed_map:
            status = fuzz_catalog.SEED_STATUS.get(seed_id)
            if status == fuzz_catalog.PENDING or status is None:
                raise CorpusProfileError(f"seed {seed_id} is unknown or pending in the authored catalog")
            raise CorpusProfileError(f"seed {seed_id} is missing from the provided seed mapping")
        seed = seed_map[seed_id]
        fuzz_catalog.validate_seed(seed)
        capability = seed.get("capability", {})
        if f2_windows or g01:
            if capability.get("status") != "capability_pending" or capability.get("f1_routable") is not False:
                raise CorpusProfileError(f"seed {seed_id}: F2 template provenance must remain pending")
        elif capability.get("status") != "authored" or capability.get("f1_routable") is not True:
            raise CorpusProfileError(
                f"seed {seed_id} is capability pending and cannot enter the F1 live corpus")
        refs = seed.get("surface_refs")
        if not routed and (not isinstance(refs, list) or len(refs) != 1):
            raise CorpusProfileError(f"seed {seed_id} must reference exactly one surface in this slice")
        if routed and (not isinstance(refs, list) or not refs):
            raise CorpusProfileError(f"seed {seed_id} must reference at least one valid template surface")
        surface_id = spec["mutation_routes"][0]["surface_id"] if routed else refs[0]
        boundary_class = spec["mutation_routes"][0]["boundary_class"] if routed else spec["boundary_class"]
        if not (f2_windows or g01) and surface_id not in fuzz_catalog.F1_SURFACE_IDS:
            raise CorpusProfileError(f"seed {seed_id} references non-F1 surface {surface_id}")
        if surface_id not in surfaces:
            raise CorpusProfileError(
                f"seed {seed_id} surface {surface_id} is not declared in the selected cycle")
        surface = (fuzz_catalog.g01_live_contract().surface_by_id["G01"] if g01 else
                   fuzz_catalog.f2_live_surface(surface_id) if f2_windows
                   else fuzz_catalog.SURFACES[surface_id])
        if not surface.executable:
            raise CorpusProfileError(f"seed {seed_id} surface {surface_id} is not executable")
        if boundary_class not in surface.boundary_classes:
            raise CorpusProfileError(
                f"seed {seed_id} boundary_class {spec['boundary_class']} is not declared by {surface_id}")

        target_id = spec["target_id"]
        if target_id not in declared:
            raise CorpusProfileError(f"seed {seed_id} target {target_id} is not explicitly exploratory")
        target = declared.get(target_id)
        if target is None:
            raise CorpusProfileError(f"seed {seed_id} target {target_id} is not declared")
        contract = target.exploration_contract
        if contract is None:
            raise CorpusProfileError(f"target {target_id} has no exploration contract")
        if surface_id not in contract.surface_by_id:
            raise CorpusProfileError(
                f"target {target_id} does not declare surface {surface_id}")
        runtime_target = ("discovery.match_compatibility" if g01 else
                          F2_WINDOWS_RUNTIME_TARGET if f2_windows
                          else EXPLORATORY_RUNTIME_TARGET.get(target_id))
        if runtime_target is None:
            raise CorpusProfileError(
                f"target {target_id} has no required runtime family; capability pending")
        if seed.get("target_family") != runtime_target:
            raise CorpusProfileError(
                f"seed {seed_id} target_family {seed.get('target_family')} "
                f"does not match {target_id} runtime family {runtime_target}")
        if target.observation_contract.get("activation") != ("F2" if f2_windows or g01 else "F1"):
            raise CorpusProfileError(f"target {target_id} activation differs from explicit profile")

        # Select from the target's *projected* contract surface, not the raw
        # catalog list: projection prunes mutators per target (for example W02
        # keeps only M03 in the late-join contract).  Picking the catalog's
        # first mutator would hand the runner an undeclared mutator id.
        projected = contract.surface_by_id[surface_id]
        implemented = (["M08"] if g01 else
                       [F2_SURFACE_MUTATORS[surface_id]] if f2_windows
                       and F2_SURFACE_MUTATORS[surface_id] in projected.mutator_ids else
                       [mid for mid in projected.mutator_ids if mid in MUTATORS])
        if f2_windows and (projected.to_dict() != surface.to_dict()
                           or implemented != [F2_SURFACE_MUTATORS[surface_id]]):
            raise CorpusProfileError(f"target {target_id} F2 surface/mutator contract mismatch")
        if not implemented:
            raise CorpusProfileError(
                f"surface {surface_id} has no implemented mutator for the live corpus")
        undeclared = [mid for mid in surface.mutator_ids
                      if mid in MUTATORS and mid not in projected.mutator_ids]
        if undeclared:
            # Not an error: catalog mutators may be pruned by this target's
            # contract.  The corpus records only the projected set.
            pass

        live_seed_variants.add(variant_id)
        contract_hash = contract.contract_hash()
        for rmw in rmws:
            entry = {
                "target_id": target_id,
                "target_revision": target.target_revision,
                "rmw": rmw,
                "contract_hash": contract_hash,
                "surface_id": surface_id,
                "boundary_class": boundary_class,
                "seed_id": seed_id,
                "variant_id": spec.get("variant_id", seed_id),
                "seed_content_sha256": fuzz_catalog.seed_content_sha256(seed),
                "seed_template": True,
                "seed_provenance_kind": seed.get("provenance", {}).get("kind"),
                "derived_from_execution": False,
                "profile_id": profile["profile_id"],
                "cycle_id": cycle["cycle_id"],
                "mutator_id": implemented[0],
                "final_config": {**deepcopy(seed["logical_config"]),
                                  **deepcopy(spec.get("overrides", {}).get("logical_config", {}))},
                "executable": True,
            }
            if routed:
                entry.update(mutation_routes_schema=ROUTES_SCHEMA,
                             mutation_routes=deepcopy(spec["mutation_routes"]))
                entry.update(spec["mutation_routes"][0])
            if f2_windows:
                # New contract-bound executable input, NOT promotion of the old
                # pending template, nor evidence of runtime guard discharge.
                entry["final_config"] = apply_profile_controls(
                    entry["final_config"], profile, backend="discovery_namespace")
                from r2f.campaign.runner import _materialize_recovery_windows
                _materialize_recovery_windows(entry["final_config"])
                entry.update(activation="F2", runtime_capability=F2_WINDOWS_CAPABILITY,
                             seed_template_capability=deepcopy(capability),
                             seed_template_status=seed.get("status"),
                             runtime_authorization={"runtime_capability": F2_WINDOWS_CAPABILITY,
                                 "target_id": target_id, "target_revision": target.target_revision,
                                 "contract_hash": contract_hash, "surface_id": surface_id,
                                 "mutator_id": implemented[0], "observation_guards_discharged": False})
            if routed:
                try:
                    validate_route_entry(entry, profile=profile)
                except ValueError as exc:
                    raise CorpusProfileError(str(exc)) from exc
            corpus.add_seed(entry)

    if not corpus.entries:
        raise CorpusProfileError("profile produced an empty corpus")
    return corpus

# ---------------------------------------------------------------------------
# Explicit F2 generation path.
#
# F2 surfaces (E02/E03/G01) and W01's relation classes are generatable but
# NOT executable: their projected core surface has an empty runtime binding and
# the shared runner keeps ``execution_admitted=False`` for them.  They must
# therefore never enter the F1 live corpus built by ``corpus_from_profile``.
# This separate builder produces *generation-only* records:
#
#   * ``executable=False`` so ``Corpus.selectable()`` never hands them to a
#     runner that would have to refuse them case by case;
#   * ``execution_admitted=False`` and ``activation="F2"`` so an accidental
#     promotion is visible in the data, not just in a comment;
#   * ``planned_only=True`` with no raw/oracle/receipt fields, so they cannot be
#     mistaken for evidence or a hit.
#
# Nothing here starts ROS, opens a socket, or writes execution evidence.
# ---------------------------------------------------------------------------

F2_GENERATION_SLICE = "f2_generation_only"


def _f2_profile_document(profile):
    profile = _load_profile_document(profile)
    if profile.get("schema_version") != _PROFILE_SCHEMA:
        raise CorpusProfileError("unsupported profile schema_version")
    if profile.get("campaign_kind") != "exploratory":
        raise CorpusProfileError("profile campaign_kind must be exploratory")
    if profile.get("activation") != "F2":
        raise CorpusProfileError(
            "profile activation must be F2 for the F2 generation path; "
            "F1 profiles use corpus_from_profile")
    if profile.get("required_catalog_included") is not False:
        raise CorpusProfileError("exploratory profile must not include the required catalog")
    if profile.get("enabled") is not False:
        raise CorpusProfileError("profile must stay enabled=false; enabling is a separate owner decision")
    if not isinstance(profile.get("profile_id"), str) or not profile["profile_id"]:
        raise CorpusProfileError("profile profile_id is required")
    if profile.get("seed_catalog_status") != "authored_t02":
        raise CorpusProfileError("profile seed_catalog_status must be authored_t02")
    return profile


def _f2_seed_specs(specs, *, context="f2 profile"):
    if not isinstance(specs, list) or not specs:
        raise CorpusProfileError(f"{context} seed_specs must be a nonempty list")
    normalized, seen = [], set()
    for index, spec in enumerate(specs):
        if not isinstance(spec, dict):
            raise CorpusProfileError(f"{context} seed_specs[{index}] must be an object")
        seed_id = spec.get("seed_id")
        target_id = spec.get("target_id")
        boundary = spec.get("boundary_class")
        mutator_id = spec.get("mutator_id")
        variant = spec.get("variant_id") or f"{seed_id}-{mutator_id}"
        if not all(isinstance(v, str) and v for v in (seed_id, target_id, boundary, mutator_id)):
            raise CorpusProfileError(
                f"{context} seed_specs[{index}] requires seed_id/target_id/boundary_class/mutator_id")
        if spec.get("executable") is not False:
            raise CorpusProfileError(
                f"{context} seed_specs[{index}] {seed_id}: F2 generation entries must declare "
                "executable=false; generation is not execution")
        key = (seed_id, target_id, variant)
        if key in seen:
            raise CorpusProfileError(f"{context} seed_specs duplicates {seed_id}/{variant}")
        seen.add(key)
        overrides = spec.get("overrides", {})
        if not isinstance(overrides, dict):
            raise CorpusProfileError(f"{context} seed_specs[{index}].overrides must be an object")
        logical = overrides.get("logical_config", {})
        if not isinstance(logical, dict):
            raise CorpusProfileError(f"{context} overrides.logical_config must be an object")
        normalized.append({"seed_id": seed_id, "target_id": target_id,
                           "variant_id": variant, "boundary_class": boundary,
                           "mutator_id": mutator_id, "executable": False,
                           "overrides": {"logical_config": deepcopy(logical)}})
    return normalized


def _f2_cycles(profile):
    """Parse F2 cycles with generation-only seed specs.

    Deliberately separate from ``_profile_cycles``: that parser enforces the F1
    rule ``executable is True`` and would reject every honest F2 entry.
    """
    cycles = profile.get("cycles")
    if not isinstance(cycles, list) or not cycles:
        raise CorpusProfileError(
            "F2 generation profile must declare explicit cycles")
    normalized, seen = [], set()
    for index, cycle in enumerate(cycles):
        if not isinstance(cycle, dict):
            raise CorpusProfileError(f"profile cycles[{index}] must be an object")
        cycle_id = cycle.get("cycle_id")
        if not isinstance(cycle_id, str) or not cycle_id:
            raise CorpusProfileError(f"profile cycles[{index}].cycle_id is required")
        if cycle_id in seen:
            raise CorpusProfileError(f"profile cycles duplicates cycle_id {cycle_id}")
        seen.add(cycle_id)
        target_id = cycle.get("target_id")
        if not isinstance(target_id, str) or not target_id:
            raise CorpusProfileError(f"profile cycle {cycle_id}.target_id is required")
        surfaces = cycle.get("surfaces")
        if not isinstance(surfaces, list) or not surfaces or any(
                not isinstance(item, str) or not item for item in surfaces):
            raise CorpusProfileError(f"profile cycle {cycle_id}.surfaces must be nonempty ids")
        if len(set(surfaces)) != len(surfaces):
            raise CorpusProfileError(f"profile cycle {cycle_id}.surfaces contains duplicates")
        specs = _f2_seed_specs(cycle.get("seed_specs"), context=f"profile cycle {cycle_id}")
        if any(spec["target_id"] != target_id for spec in specs):
            raise CorpusProfileError(
                f"profile cycle {cycle_id} contains a seed for a different target")
        normalized.append({"cycle_id": cycle_id, "target_id": target_id,
                           "surfaces": list(surfaces), "seed_specs": specs})
    return normalized


def f2_generation_corpus_from_profile(profile, seeds=None, *, cycle_id=None):
    """Build a generation-only corpus for F2 surfaces (never an F1 live corpus).

    This is an explicit, separately-named path.  ``corpus_from_profile`` keeps
    rejecting F2 surfaces, so F1 selection cannot silently pick these up.
    """
    from r2f.modules.s1 import fuzz_catalog
    from r2f.modules.s1.targets import (
        EXPLORATORY_RUNTIME_TARGET, EXPLORATORY_TARGET_ORDER,
        exploratory_target_declarations,
    )
    from r2f.core.mutators import F2_MUTATORS

    profile = _f2_profile_document(profile)
    rmws = _profile_strings(profile, "rmws")
    all_surfaces = _profile_strings(profile, "surfaces")
    all_targets = _profile_strings(profile, "target_ids")
    cycles = _f2_cycles(profile)
    if cycle_id is None:
        raise CorpusProfileError("explicit cycle_id is required; refusing a mixed generation corpus")
    matches = [cycle for cycle in cycles if cycle["cycle_id"] == cycle_id]
    if len(matches) != 1:
        raise CorpusProfileError(f"profile has no unique cycle matching {cycle_id!r}")
    cycle = matches[0]
    if cycle["target_id"] not in all_targets:
        raise CorpusProfileError(
            f"cycle {cycle_id} target {cycle['target_id']} is not in profile target_ids")

    for surface_id in all_surfaces:
        if surface_id not in fuzz_catalog.SURFACES:
            raise CorpusProfileError(f"profile surfaces contains unknown surface {surface_id}")
        if surface_id not in fuzz_catalog.F2_GENERATION_MUTATORS:
            raise CorpusProfileError(
                f"profile surface {surface_id} has no F2 generation path "
                f"(generatable: {','.join(sorted(fuzz_catalog.F2_GENERATION_MUTATORS))})")
        projected = fuzz_catalog.f2_generation_surface(surface_id)
        if projected.executable:
            raise CorpusProfileError(
                f"surface {surface_id} projected as executable; F2 generation must not be")
    seed_map = seeds
    if seed_map is None:
        seed_map = fuzz_catalog.load_seeds()["seeds"]
    declared = exploratory_target_declarations()
    corpus = Corpus()
    live_variants = set()
    for spec in cycle["seed_specs"]:
        seed_id = spec["seed_id"]
        if seed_id not in seed_map:
            raise CorpusProfileError(f"seed {seed_id} is missing from the provided seed mapping")
        seed = seed_map[seed_id]
        fuzz_catalog.validate_seed(seed)
        surface_id = (seed.get("surface_refs") or [None])[0]
        if surface_id not in fuzz_catalog.F2_GENERATION_MUTATORS:
            raise CorpusProfileError(
                f"seed {seed_id} surface {surface_id} has no F2 generation path")
        if surface_id not in all_surfaces:
            raise CorpusProfileError(
                f"seed {seed_id} surface {surface_id} is not declared in the selected cycle")
        if spec["mutator_id"] not in fuzz_catalog.F2_GENERATION_MUTATORS[surface_id]:
            raise CorpusProfileError(
                f"seed {seed_id} mutator {spec['mutator_id']} is not declared for {surface_id}")
        if spec["mutator_id"] not in F2_MUTATORS:
            raise CorpusProfileError(f"mutator {spec['mutator_id']} is not implemented")
        surface = fuzz_catalog.SURFACES[surface_id]
        if spec["boundary_class"] not in surface.boundary_classes:
            raise CorpusProfileError(
                f"seed {seed_id} boundary_class {spec['boundary_class']} is not declared by {surface_id}")
        target_id = spec["target_id"]
        if target_id not in EXPLORATORY_TARGET_ORDER:
            raise CorpusProfileError(f"seed {seed_id} target {target_id} is not exploratory")
        if target_id != cycle["target_id"]:
            raise CorpusProfileError(
                f"seed {seed_id} target {target_id} does not match cycle target {cycle['target_id']}")
        # Generation never opts in to the distinct F2 windows runtime target.
        runtime_target = EXPLORATORY_RUNTIME_TARGET.get(target_id)
        # A target with no required runtime family (G01) is still generatable:
        # generation is not execution.  When a family is declared, the seed must
        # agree with it; the family is never invented here.
        if runtime_target is not None and seed.get("target_family") != runtime_target:
            raise CorpusProfileError(
                f"seed {seed_id} target_family {seed.get('target_family')} "
                f"does not match {target_id} runtime family {runtime_target}")
        if runtime_target is None and not isinstance(seed.get("target_family"), str):
            raise CorpusProfileError(f"seed {seed_id} must declare a target_family")
        if declared.get(target_id) is None:
            raise CorpusProfileError(f"target {target_id} is not declared")
        live_variants.add(spec["variant_id"])
        target_contract = declared[target_id].exploration_contract
        if target_contract is None:
            raise CorpusProfileError(f"target {target_id} has no exploration contract")
        for rmw in rmws:
            entry = {
                "target_id": target_id,
                "target_revision": declared[target_id].target_revision,
                "rmw": rmw,
                "contract_hash": target_contract.contract_hash(),
                "surface_id": surface_id,
                "boundary_class": spec["boundary_class"],
                "seed_id": seed_id,
                "variant_id": spec["variant_id"],
                "seed_content_sha256": fuzz_catalog.seed_content_sha256(seed),
                "seed_template": True,
                "seed_provenance_kind": seed.get("provenance", {}).get("kind"),
                "derived_from_execution": False,
                "profile_id": profile["profile_id"],
                "cycle_id": cycle["cycle_id"],
                "mutator_id": spec["mutator_id"],
                "final_config": {**deepcopy(seed["logical_config"]),
                                  **deepcopy(spec["overrides"]["logical_config"])},
                # Generation-only.  Never selectable, never a hit, never a run.
                "executable": False,
                "execution_admitted": False,
                "activation": "F2",
                "planned_only": True,
                "capability_status": "generatable_pending_execution",
                "runner_slice": F2_GENERATION_SLICE,
            }
            corpus.add_seed(entry)
    if not corpus.entries:
        raise CorpusProfileError("F2 profile produced an empty generation corpus")
    return corpus


TOPOLOGY_NORMALIZATION_SCHEMA = "r2f.topology-seed-normalization/v1"
TOPOLOGY_INGESTION_SCHEMA = "r2f.topology-seed-ingestion/v1"


def normalize_topology_seed_corpus(source_corpus):
    """Return authored topology entries and a disposition for EVERY source entry.

    Hashes are checked in their original domains. No defaults, RNG or observed
    data are injected. Unsupported surfaces remain named in the ingestion audit.
    """
    from r2f.modules.s1 import input_model, topology_model
    from .exploratory_input import TOPOLOGY_SOURCE_SCHEMA
    if not isinstance(source_corpus, dict) or source_corpus.get('schema') != TOPOLOGY_SOURCE_SCHEMA:
        raise CorpusProfileError('topology_source_schema_mismatch')
    entries = source_corpus.get('entries')
    if not isinstance(entries, list) or len(entries) > 256:
        raise CorpusProfileError('topology_source_entries_budget_or_shape')
    normalized, rows, seen = {}, [], set()
    for entry in entries:
        if not isinstance(entry, dict) or entry.get('schema') != 'r2f.s1-seed-entry/v1':
            raise CorpusProfileError('topology_source_entry_schema_mismatch')
        seed_id, surface = entry.get('seed_id'), entry.get('surface_id')
        if not isinstance(seed_id, str) or not seed_id or seed_id in seen:
            raise CorpusProfileError('topology_source_seed_id_missing_or_duplicate:' + str(seed_id))
        seen.add(seed_id)
        if 'document' not in entry or entry.get('content_sha256') != input_model.content_hash(entry['document']):
            raise CorpusProfileError('topology_source_document_hash_mismatch:' + seed_id)
        row = {'seed_id': seed_id, 'surface_id': surface,
               'source_content_sha256': entry['content_sha256'],
               'disposition': 'not_connected_non_topology_surface'}
        if surface != 'S1.topology':
            rows.append(row)
            continue
        provenance = entry.get('provenance', {})
        if not isinstance(provenance, dict) or entry.get('status') != 'authored' or provenance.get('authored') is not True or \
                provenance.get('derived_from_execution') is not False or \
                entry.get('execution_evidence') is not False or entry.get('candidate_eligible') is not False:
            raise CorpusProfileError('topology_source_not_authored_input:' + seed_id)
        program = entry['document']
        if not isinstance(program, dict) or program.get('schema') != topology_model.SCHEMA or \
                len(json.dumps(program, ensure_ascii=False, allow_nan=False).encode()) > 1024 * 1024:
            raise CorpusProfileError('topology_source_program_shape_or_byte_budget:' + seed_id)
        expected = entry.get('expected_disposition')
        validation = topology_model.validate_program(program)
        negative = expected == 'expect_guard_reject'
        if expected not in ('expect_pass', 'expect_guard_reject') or (
                not negative and (validation['status'] != 'valid' or
                                  validation['guard_disposition'] == 'guard_violation')) or (
                negative and validation['status'] == 'valid' and
                validation['guard_disposition'] != 'guard_violation') or (
                validation['status'] != 'valid' and (not negative or any(
                    error['code'] != 'duplicate' for error in validation['errors']))):
            raise CorpusProfileError('topology_seed_validation_or_guard_expectation_mismatch:' + seed_id)
        row['disposition'] = 'retained_guard_negative' if negative else 'normalized_authored_topology'
        normalized[seed_id] = {**deepcopy(entry), 'validation': validation}
        rows.append(row)
    body = {'schema': TOPOLOGY_INGESTION_SCHEMA, 'source_schema': TOPOLOGY_SOURCE_SCHEMA,
            'source_entry_count': len(entries), 'entries': rows,
            'execution_evidence': False, 'candidate_eligible': False}
    return normalized, {**body, 'receipt_sha256': canonical_hash(body)}


def _validate_topology_ingestion(receipt, records):
    """Check detached normalization provenance on checkpoint restore."""
    from r2f.modules.s1 import input_model
    if not isinstance(receipt, dict) or receipt.get('schema') != TOPOLOGY_INGESTION_SCHEMA:
        raise ValueError('topology_ingestion_receipt_schema_mismatch')
    body = {key: value for key, value in receipt.items() if key != 'receipt_sha256'}
    if canonical_hash(body) != receipt.get('receipt_sha256'):
        raise ValueError('topology_ingestion_receipt_hash_mismatch')
    if receipt.get('source_schema') != 'r2f.s1-seed-corpus/v2' or \
            receipt.get('execution_evidence') is not False or receipt.get('candidate_eligible') is not False:
        raise ValueError('topology_ingestion_contract_mismatch')
    rows = receipt.get('entries')
    if not isinstance(rows, list) or receipt.get('source_entry_count') != len(rows):
        raise ValueError('topology_ingestion_entry_count_mismatch')
    for row in rows:
        if not isinstance(row, dict) or set(row) != {
                'seed_id', 'surface_id', 'source_content_sha256', 'disposition'} or \
                not isinstance(row['seed_id'], str) or not row['seed_id'] or \
                not isinstance(row['source_content_sha256'], str) or \
                re.fullmatch(r'[0-9a-f]{64}', row['source_content_sha256']) is None or \
                row['disposition'] not in {'not_connected_non_topology_surface',
                                          'normalized_authored_topology', 'retained_guard_negative'} or \
                (row['surface_id'] == 'S1.topology') != (
                    row['disposition'] != 'not_connected_non_topology_surface'):
            raise ValueError('topology_ingestion_entry_contract_mismatch')
    by_id = {row['seed_id']: row for row in rows}
    if len(by_id) != len(rows):
        raise ValueError('topology_ingestion_duplicate_seed_id')
    for record in records:
        normalization = record.get('normalization_receipt')
        # Derived corpus observations have their own receipts; only source
        # templates participate in the authored normalization contract.
        if normalization is None:
            if record.get('seed_template') is True or record.get('source_seed_id') is not None:
                raise ValueError('topology_source_normalization_receipt_missing')
            continue
        if not isinstance(normalization, dict) or normalization.get('schema') != TOPOLOGY_NORMALIZATION_SCHEMA:
            raise ValueError('topology_normalization_receipt_schema_mismatch')
        if normalization.get('source_surface_id') != 'S1.topology' or \
                normalization.get('core_surface_id') != 'S1.topology.program' or \
                normalization.get('normalization') != 'wrap_document_without_defaults_or_mutation' or \
                normalization.get('execution_evidence') is not False or \
                normalization.get('candidate_eligible') is not False:
            raise ValueError('topology_normalization_contract_mismatch')
        row = by_id.get(record.get('source_seed_id'))
        final = record.get('final_config', {})
        if row is None or row['disposition'] == 'not_connected_non_topology_surface' or \
                record.get('source_schema') != receipt.get('source_schema') or \
                record.get('source_content_sha256') != row['source_content_sha256'] or \
                normalization.get('source_ingestion_sha256') != receipt['receipt_sha256'] or \
                normalization.get('source_content_sha256') != row['source_content_sha256'] or \
                set(final) != {'topology_program'} or \
                input_model.content_hash(final['topology_program']) != row['source_content_sha256'] or \
                normalization.get('core_envelope_sha256') != canonical_hash(final):
            raise ValueError('topology_source_normalization_binding_mismatch')
        provenance = record.get('source_provenance')
        if record.get('source_entry_schema') != 'r2f.s1-seed-entry/v1' or \
                not isinstance(provenance, dict) or provenance.get('authored') is not True or \
                provenance.get('derived_from_execution') is not False or \
                record.get('planned_only') is not True or record.get('derived_from_execution') is not False:
            raise ValueError('topology_authored_source_provenance_mismatch')
        negative = row['disposition'] == 'retained_guard_negative'
        if record.get('expected_disposition') != ('expect_guard_reject' if negative else 'expect_pass'):
            raise ValueError('topology_source_expected_disposition_mismatch')
        if record.get('case_kind') != ('guard_negative' if negative else 'authored_topology') or \
                record.get('executable') is not (not negative) or \
                record.get('candidate_eligible') is not False or record.get('execution_admitted') is not False:
            raise ValueError('topology_guard_or_candidate_classification_mismatch')


def topology_corpus_from_profile(profile, source_corpus=None, *, cycle_id=None):
    """Bridge authored v2 programs into the existing Corpus; no execution."""
    from r2f.modules.s1 import fuzz_mutators_v2
    from r2f.modules.s1.topology_targets import (
        TOPOLOGY_TARGET, TOPOLOGY_SURFACE, TOPOLOGY_MUTATOR, TOPOLOGY_MUTATION_KIND,
        TOPOLOGY_CARRIER, topology_target_declaration)
    from .exploratory_input import validate_topology_profile, TOPOLOGY_SOURCE_SCHEMA
    profile = _load_profile_document(profile)
    try:
        validate_topology_profile(profile)
    except ValueError as exc:
        raise CorpusProfileError(str(exc)) from exc
    if cycle_id is None:
        if len(profile['cycles']) != 1:
            raise CorpusProfileError('topology_explicit_cycle_id_required')
        cycle = profile['cycles'][0]
    else:
        matches = [c for c in profile['cycles'] if c['cycle_id'] == cycle_id]
        if len(matches) != 1:
            raise CorpusProfileError('topology_cycle_not_found:' + str(cycle_id))
        cycle = matches[0]
    source = fuzz_mutators_v2.corpus_v2() if source_corpus is None else source_corpus
    normalized, ingestion = normalize_topology_seed_corpus(source)
    dispositions = {row['seed_id']: row for row in ingestion['entries']}
    target = topology_target_declaration()
    corpus = Corpus(**profile.get('corpus', {}))
    corpus.source_ingestion_receipt = ingestion
    for spec in cycle['seed_specs']:
        seed_id = spec['seed_id']
        if seed_id not in normalized:
            reason = dispositions.get(seed_id, {}).get('disposition', 'missing_seed')
            raise CorpusProfileError('topology_seed_not_connected:' + seed_id + ':' + reason)
        seed = normalized[seed_id]
        if spec['expected_disposition'] != seed['expected_disposition']:
            raise CorpusProfileError('topology_seed_spec_expectation_mismatch:' + seed_id)
        final = {'topology_program': deepcopy(seed['document'])}
        normalization = {'schema': TOPOLOGY_NORMALIZATION_SCHEMA,
                         'source_surface_id': 'S1.topology', 'core_surface_id': TOPOLOGY_SURFACE,
                         'source_content_sha256': seed['content_sha256'],
                         'core_envelope_sha256': canonical_hash(final),
                         'source_ingestion_sha256': ingestion['receipt_sha256'],
                         'normalization': 'wrap_document_without_defaults_or_mutation',
                         'execution_evidence': False, 'candidate_eligible': False}
        for rmw in profile['rmws']:
            corpus.add_seed({
                'target_id': TOPOLOGY_TARGET, 'target_revision': target.target_revision, 'rmw': rmw,
                'contract_hash': target.exploration_contract.contract_hash(),
                'profile_id': profile['profile_id'], 'cycle_id': cycle['cycle_id'],
                'surface_id': TOPOLOGY_SURFACE, 'boundary_class': spec['boundary_class'],
                'mutator_id': TOPOLOGY_MUTATOR, 'mutation_kind': TOPOLOGY_MUTATION_KIND,
                'carrier': TOPOLOGY_CARRIER, 'seed_id': seed_id, 'seed_template': True,
                'source_schema': TOPOLOGY_SOURCE_SCHEMA, 'source_entry_schema': seed['schema'],
                'source_seed_id': seed_id, 'source_content_sha256': seed['content_sha256'],
                'source_provenance': deepcopy(seed['provenance']),
                'normalization_receipt': deepcopy(normalization), 'final_config': deepcopy(final),
                'expected_disposition': seed['expected_disposition'],
                'case_kind': 'guard_negative' if not spec['executable'] else 'authored_topology',
                'executable': spec['executable'], 'execution_admitted': False,
                'candidate_eligible': False, 'derived_from_execution': False,
                'planned_only': True, 'runner_slice': 'topology_v1',
            })
    _validate_topology_ingestion(ingestion, corpus.entries.values())
    return corpus
