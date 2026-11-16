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

from r2f.core.evidence import canonical_hash

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

# Only deterministic input content contributes to corpus identity.  Observation
# labels, retention metadata, random scheduling values and execution identity
# are excluded by construction rather than by a growing blacklist.  A caller may
# attach any of those labels without splitting an otherwise identical input.
INPUT_IDENTITY_KEYS = (
    "target_id", "target_revision", "rmw", "contract_hash", "profile_id",
    "mutation_kind", "carrier", "mutation_parameters", "operations",
    "mutation_plan", "runtime_windows", "contract", "input_layer_receipts",
    "final_config", "final_input",
)


def _strip_transient(value):
    """Drop execution metadata from whitelisted input subtrees recursively."""
    if isinstance(value, dict):
        return {key: _strip_transient(item) for key, item in value.items() if key not in _TRANSIENT}
    if isinstance(value, list):
        return [_strip_transient(item) for item in value]
    return deepcopy(value)


_TRANSIENT = frozenset({
    "run_id", "campaign_id", "domain", "domain_id", "origin_domain", "time",
    "timestamp", "wallclock", "started_at", "finished_at", "duration_sec",
    "guid", "pid", "endpoint_guid", "endpoint_generation", "repeat", "rng_seed",
    "selection_id", "case_id", "cell_id", "output_dir", "raw_path",
    "raw_sha256", "manifest_path", "history", "retained", "evicted",
})


def content_identity(entry):
    """Deterministic semantic identity of a corpus input.

    Only whitelisted input fields participate.  observed_edge, surface_id,
    boundary_class, relation_class, contract_branch, oracle_verdict, seed_id,
    pinned_seed, source, bucket_key, admission_reason, run/domain/time,
    repeat/RNG seed, GUID/PID and paths are observation or retention metadata
    and never split otherwise identical content.
    """
    if not isinstance(entry, dict):
        raise ValueError("corpus entry must be a dict")
    identity = {key: _strip_transient(entry[key]) for key in INPUT_IDENTITY_KEYS if key in entry}
    if "final_input" in identity and "final_config" not in identity:
        identity["final_config"] = identity.pop("final_input")
    elif "final_input" in identity:
        # Keep one canonical spelling if both names are present.
        identity.pop("final_input")
    required = ("target_id", "target_revision", "rmw", "contract_hash", "final_config")
    if any(key not in identity for key in required):
        raise ValueError("corpus content identity requires target/revision/rmw/contract/final config")
    if not isinstance(identity["final_config"], dict) or not identity["final_config"]:
        raise ValueError("corpus final_config must be a nonempty dict")
    return canonical_hash({"schema": SCHEMA, "identity": identity})


def content_sha256(entry):
    return entry.get("content_sha256") or content_identity(entry)


def bucket_key(entry):
    edge = entry.get("observed_edge") or ""
    return (entry.get("target_id"), entry.get("boundary_class") or "", edge)


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
    record = deepcopy(entry)
    record["content_sha256"] = content_sha256(entry)
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
        if bucket is not None:
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

    def to_dict(self):
        return {"schema": SCHEMA, "capacity": self.capacity, "bucket_limit": self.bucket_limit,
                "entry_count": len(self.entries), "evictable_count": len(self._evictable()),
                "pinned_seed_count": sum(r.get("pinned_seed") is True for r in self.entries.values()),
                "candidate_reference_count": len(self.candidates),
                "capacity_controlled": len(self._evictable()) <= self.capacity,
                "entries": sorted((deepcopy(r) for r in self.entries.values()),
                                  key=lambda r: r["content_sha256"]),
                "history": deepcopy(self.history)}


def corpus_identity(corpus):
    """Index identity; excludes history order and execution metadata."""
    if isinstance(corpus, Corpus):
        corpus = corpus.to_dict()
    members = sorted(({key: row.get(key) for key in
                       ("content_sha256", "target_id", "seed_id", "pinned_seed", "candidate_ids")}
                      for row in corpus.get("entries", [])),
                     key=lambda r: r["content_sha256"])
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


def _runner_cycle_targets():
    """Targets the shared exploratory runner can currently turn into cases.

    Imported lazily to avoid an import cycle (the runner imports this module).
    The set is the runner's own gate; this module never widens it, it only
    refuses to build a corpus the runner would reject case by case.
    """
    from r2f.campaign.runner import _F1_CYCLE_TARGETS
    return frozenset(_F1_CYCLE_TARGETS)


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


def _profile_seed_specs(specs, *, context="profile"):
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
        key = (seed_id, target_id)
        if key in seen:
            raise CorpusProfileError(f"{context} seed_specs duplicates {seed_id} for {target_id}")
        seen.add(key)
        normalized.append({"seed_id": seed_id, "target_id": target_id,
                           "boundary_class": boundary, "executable": True})
    return normalized


def _profile_cycles(profile):
    """Resolve explicit per-target cycles.

    A single profile may declare several targets, but each cycle is one target
    plus its own seed list.  Building a corpus requires choosing exactly one
    cycle, so a mixed corpus can never be handed to an endpoint-pair runner.
    """
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
                                   context=f"profile cycle {cycle_id}")
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
    supported = _runner_cycle_targets()
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

    F1-routable templates only enter the corpus.  Capability-pending F2
    surfaces/seeds (E02, E03, G01) are rejected if they appear as live
    surfaces or seed specs; they may only be listed as pending metadata.
    """
    from r2f.modules.s1 import fuzz_catalog
    from r2f.modules.s1.targets import (
        EXPLORATORY_RUNTIME_TARGET, EXPLORATORY_TARGET_ORDER,
        exploratory_target_declarations,
    )
    from r2f.core.mutators import MUTATORS

    profile = _load_profile_document(profile)
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
    if profile.get("enabled") is not False:
        raise CorpusProfileError("profile must stay enabled=false; enabling is a separate owner decision")

    all_targets = _profile_strings(profile, "target_ids")
    all_surfaces = _profile_strings(profile, "surfaces")
    rmws = _profile_strings(profile, "rmws")
    cycles = _profile_cycles(profile)

    for target in all_targets:
        if target not in EXPLORATORY_TARGET_ORDER:
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

    for surface_id in all_surfaces:
        if surface_id not in fuzz_catalog.SURFACES:
            raise CorpusProfileError(f"profile surfaces contains unknown surface {surface_id}")
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

    supported = _runner_cycle_targets()
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

    declared_target = exploratory_target_declarations().get(cycle["target_id"])
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

    declared = exploratory_target_declarations()
    live_seed_ids = set()
    for spec in specs:
        seed_id = spec["seed_id"]
        if seed_id in live_seed_ids:
            raise CorpusProfileError(f"seed {seed_id} is declared twice in the live corpus")
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
        if capability.get("status") != "authored" or capability.get("f1_routable") is not True:
            raise CorpusProfileError(
                f"seed {seed_id} is capability pending and cannot enter the F1 live corpus")
        refs = seed.get("surface_refs")
        if not isinstance(refs, list) or len(refs) != 1:
            raise CorpusProfileError(f"seed {seed_id} must reference exactly one surface in this slice")
        surface_id = refs[0]
        if surface_id not in fuzz_catalog.F1_SURFACE_IDS:
            raise CorpusProfileError(f"seed {seed_id} references non-F1 surface {surface_id}")
        if surface_id not in surfaces:
            raise CorpusProfileError(
                f"seed {seed_id} surface {surface_id} is not declared in the selected cycle")
        surface = fuzz_catalog.SURFACES[surface_id]
        if not surface.executable:
            raise CorpusProfileError(f"seed {seed_id} surface {surface_id} is not executable")
        if spec["boundary_class"] not in surface.boundary_classes:
            raise CorpusProfileError(
                f"seed {seed_id} boundary_class {spec['boundary_class']} is not declared by {surface_id}")

        target_id = spec["target_id"]
        if target_id not in EXPLORATORY_TARGET_ORDER:
            raise CorpusProfileError(f"seed {seed_id} target {target_id} is not exploratory")
        target = declared.get(target_id)
        if target is None:
            raise CorpusProfileError(f"seed {seed_id} target {target_id} is not declared")
        contract = target.exploration_contract
        if contract is None:
            raise CorpusProfileError(f"target {target_id} has no exploration contract")
        if surface_id not in contract.surface_by_id:
            raise CorpusProfileError(
                f"target {target_id} does not declare surface {surface_id}")
        runtime_target = EXPLORATORY_RUNTIME_TARGET.get(target_id)
        if runtime_target is None:
            raise CorpusProfileError(
                f"target {target_id} has no required runtime family; capability pending")
        if seed.get("target_family") != runtime_target:
            raise CorpusProfileError(
                f"seed {seed_id} target_family {seed.get('target_family')} "
                f"does not match {target_id} runtime family {runtime_target}")
        if target.observation_contract.get("activation") != "F1":
            raise CorpusProfileError(f"target {target_id} is not an F1 activation target")

        # Select from the target's *projected* contract surface, not the raw
        # catalog list: projection prunes mutators per target (for example W02
        # keeps only M03 in the late-join contract).  Picking the catalog's
        # first mutator would hand the runner an undeclared mutator id.
        projected = contract.surface_by_id[surface_id]
        implemented = [mid for mid in projected.mutator_ids if mid in MUTATORS]
        if not implemented:
            raise CorpusProfileError(
                f"surface {surface_id} has no implemented mutator for the live corpus")
        undeclared = [mid for mid in surface.mutator_ids
                      if mid in MUTATORS and mid not in projected.mutator_ids]
        if undeclared:
            # Not an error: catalog mutators may be pruned by this target's
            # contract.  The corpus records only the projected set.
            pass

        live_seed_ids.add(seed_id)
        contract_hash = contract.contract_hash()
        for rmw in rmws:
            entry = {
                "target_id": target_id,
                "target_revision": target.target_revision,
                "rmw": rmw,
                "contract_hash": contract_hash,
                "surface_id": surface_id,
                "boundary_class": spec["boundary_class"],
                "seed_id": seed_id,
                "seed_content_sha256": fuzz_catalog.seed_content_sha256(seed),
                "seed_template": True,
                "seed_provenance_kind": seed.get("provenance", {}).get("kind"),
                "derived_from_execution": False,
                "profile_id": profile["profile_id"],
                "cycle_id": cycle["cycle_id"],
                "mutator_id": implemented[0],
                "final_config": deepcopy(seed["logical_config"]),
                "executable": True,
            }
            corpus.add_seed(entry)

    if not corpus.entries:
        raise CorpusProfileError("profile produced an empty corpus")
    return corpus
